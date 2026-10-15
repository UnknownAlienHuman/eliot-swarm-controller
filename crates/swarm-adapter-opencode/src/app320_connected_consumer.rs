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
    platform::{load_credential, private_owned_directory},
    workspace::WorkspaceProjectConfig,
};
use tokio::process::{Child, ChildStdin, Command as TokioCommand};

type FixtureResult<T> = std::result::Result<T, String>;

const PLUGIN_MODULE: &[u8] = include_bytes!("../../../modules/opencode/native-mcp-proof.mjs");
const PLUGIN_ENTRY: &[u8] = include_bytes!("../../../modules/opencode/index.mjs");
const PACKAGE_JSON: &[u8] = include_bytes!("../../../modules/opencode/package.json");
const PACKAGE_LOCK: &[u8] = include_bytes!("../../../modules/opencode/package-lock.json");
const ROUTE_ALIAS: &str = "app320-connected-fixture";
const PROJECT_ID: &str = "app320-owned-launch";
const HOST_CHILD: &str = "app320_connected_consumer::app320_host_startup_child";
const ADAPTER_CHILD: &str = "app320_connected_consumer::app320_owned_adapter_child";
const FAKE_NATIVE_VERSION: &str = "2.0.7";

#[derive(Clone)]
struct ExternalNativeRegistration {
    url: String,
    pid: u32,
    version: String,
}

struct ExternalNativeProcess {
    child: Child,
    stdin: Option<ChildStdin>,
    registration_file: PathBuf,
    observation_file: PathBuf,
    stop_file: PathBuf,
    pid: u32,
    url: String,
    version: String,
}

#[derive(serde::Serialize)]
struct CaseEvidence {
    scenario: &'static str,
    operation_id: String,
    binding_id: String,
    generation: i64,
    workspace_directory: PathBuf,
    native_pid: u32,
    native_url: String,
    native_version: String,
    outcome: Value,
    operation_readback: Value,
    observed_outcomes: Vec<Value>,
    acknowledged_sha256: String,
    pending_count: usize,
    root_posts: u64,
    connection_absent: bool,
    native_listener_gone: bool,
    external_process_alive_after_adapter_stop: bool,
    native_stop_confirmed: bool,
    ipc_listener_gone: bool,
}

fn read_external_registration(path: &Path) -> FixtureResult<ExternalNativeRegistration> {
    let value: Value =
        serde_json::from_slice(&fs::read(path).map_err(display)?).map_err(display)?;
    let object = value
        .as_object()
        .ok_or_else(|| "external native registration is not an object".to_owned())?;
    let url = object
        .get("url")
        .and_then(Value::as_str)
        .filter(|url| !url.is_empty())
        .ok_or_else(|| "external native registration omitted url".to_owned())?
        .to_owned();
    let parsed = reqwest::Url::parse(&url).map_err(display)?;
    if parsed.scheme() != "http"
        || !matches!(parsed.host_str(), Some("127.0.0.1") | Some("localhost"))
        || parsed.port().is_none()
    {
        return Err("external native registration url is not a loopback HTTP endpoint".to_owned());
    }
    let pid = object
        .get("pid")
        .and_then(Value::as_u64)
        .and_then(|pid| u32::try_from(pid).ok())
        .filter(|pid| *pid > 0)
        .ok_or_else(|| "external native registration omitted a valid pid".to_owned())?;
    let version = object
        .get("version")
        .and_then(Value::as_str)
        .filter(|version| {
            !version.is_empty()
                && version.len() <= 64
                && !version.bytes().any(|byte| byte.is_ascii_control())
        })
        .ok_or_else(|| "external native registration omitted version".to_owned())?
        .to_owned();
    if let Some(password) = object.get("password")
        && !password.is_string()
    {
        return Err("external native registration password is not a string".to_owned());
    }
    Ok(ExternalNativeRegistration { url, pid, version })
}

async fn spawn_external_native(
    bun: &Path,
    server_program: &Path,
    state_root: &Path,
    registration_file: &Path,
    workspace_directory: &Path,
) -> FixtureResult<ExternalNativeProcess> {
    let password = format!("app299-native-{}", uuid::Uuid::new_v4().simple());
    let mut child = TokioCommand::new(bun)
        .arg(server_program)
        .arg("--state-root")
        .arg(state_root)
        .arg("--registration-file")
        .arg(registration_file)
        .arg("--workspace-directory")
        .arg(workspace_directory)
        .arg("--port")
        .arg("0")
        .env("ELIOT_APP299_FAKE_HTTP_PASSWORD", password)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(false)
        .spawn()
        .map_err(display)?;
    let pid = match child.id() {
        Some(pid) => pid,
        None => {
            let _ = child.kill().await;
            let _ = child.wait().await;
            return Err("external fake native process did not expose its pid".to_owned());
        }
    };
    let stdin = match child.stdin.take() {
        Some(stdin) => stdin,
        None => {
            let _ = child.kill().await;
            let _ = child.wait().await;
            return Err("external fake native process stdin is unavailable".to_owned());
        }
    };
    let registration_result = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Some(status) = child.try_wait().map_err(display)? {
                return Err(format!(
                    "external fake native process exited before registration: {status}"
                ));
            }
            if let Ok(registration) = read_external_registration(registration_file)
                && registration.pid == pid
                && registration.version == FAKE_NATIVE_VERSION
            {
                break Ok::<_, String>(registration);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    let registration = match registration_result {
        Ok(Ok(registration)) => registration,
        Ok(Err(error)) => {
            let _ = child.kill().await;
            let _ = child.wait().await;
            return Err(error);
        }
        Err(_) => {
            let _ = child.kill().await;
            let _ = child.wait().await;
            return Err("external fake native process did not publish registration".to_owned());
        }
    };
    Ok(ExternalNativeProcess {
        child,
        stdin: Some(stdin),
        registration_file: registration_file.to_owned(),
        observation_file: state_root.join("fixture-observation.json"),
        stop_file: state_root.join("stop.json"),
        pid,
        url: registration.url,
        version: registration.version,
    })
}

async fn stop_external_native(native: &mut ExternalNativeProcess) -> FixtureResult<Value> {
    drop(native.stdin.take());
    let status = match tokio::time::timeout(Duration::from_secs(60), native.child.wait()).await {
        Ok(status) => status.map_err(display)?,
        Err(_) => {
            // This is the exact fake child created by this fixture, not the
            // user's registered service. Failure cleanup never proves success.
            native.child.kill().await.map_err(display)?;
            native.child.wait().await.map_err(display)?;
            return Err(
                "external fake native process required forced cleanup after EOF".to_owned(),
            );
        }
    };
    if !status.success() {
        return Err(format!(
            "external fake native process exited unsuccessfully: {status}"
        ));
    }
    let stop: Value =
        serde_json::from_slice(&fs::read(&native.stop_file).map_err(display)?).map_err(display)?;
    let connection_absent = matches!(
        fs::symlink_metadata(&native.registration_file),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound
    );
    if !connection_absent
        || stop["status"] != json!("stopped")
        || stop["pid"] != json!(native.pid)
        || stop["connection_absent"] != json!(true)
        || stop["process_exit_code"] != json!(0)
    {
        return Err("external fake native stop receipt is not for the exact process".to_owned());
    }
    Ok(stop)
}

// These ignored helpers are subprocess roles, not independently qualifying tests.
// Only the public consumer above creates their private inputs and checks their results.
#[tokio::test]
#[ignore = "spawned with pipe stdin by the APP320 connected consumer only"]
async fn app320_host_startup_child() {
    let path = std::env::var_os("ELIOT_APP320_HOST_CONFIG")
        .map(PathBuf::from)
        .expect("APP320 host child requires its private config path");
    let diagnostic = path.with_file_name("host-startup-diagnostic.json");
    let stage = |phase: &str, code: Option<&str>| {
        let safe_code = code.filter(|value| fixture_diagnostic_code(value));
        fs::write(
            &diagnostic,
            serde_json::to_vec(&json!({"phase":phase,"code":safe_code})).unwrap(),
        )
        .expect("private bounded startup diagnostic");
    };
    stage("config_read", None);
    let mut config: HostConfig = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    // The public embedding accepts Config. Resolve the configured Forge/workspace
    // paths just as the foreground loader does; startup itself creates authority.
    stage("forge_paths", None);
    config.forge.resolve_paths(path.parent().unwrap()).unwrap();
    stage("workspace_paths", None);
    config
        .workspace
        .resolve_paths(path.parent().unwrap())
        .unwrap();
    stage("workspace_validate", None);
    config.workspace.validate(&config.forge).unwrap();
    stage("host_run", None);
    match swarm_kernel_host::host::run_on_stdin_eof(config).await {
        Ok(()) => stage("host_closed", None),
        Err(error) => {
            stage("host_failed", Some(&error.code));
            panic!("APP320 host failed; bounded phase/code retained privately");
        }
    }
}

fn fixture_diagnostic_code(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
}

fn startup_diagnostic(root: &Path) -> String {
    let diagnostic = fs::read(root.join("host-startup-diagnostic.json"))
        .ok()
        .filter(|bytes| bytes.len() <= 1024)
        .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok());
    let phase = diagnostic
        .as_ref()
        .and_then(|value| value["phase"].as_str())
        .filter(|phase| {
            [
                "config_read",
                "forge_paths",
                "workspace_paths",
                "workspace_validate",
                "host_run",
                "host_closed",
                "host_failed",
            ]
            .contains(phase)
        })
        .unwrap_or("unobserved");
    let code = diagnostic
        .as_ref()
        .and_then(|value| value["code"].as_str())
        .filter(|code| fixture_diagnostic_code(code))
        .unwrap_or("UNOBSERVED");
    format!("phase={phase},code={code}")
}

async fn supervisor_diagnostic(root: &Path, operator: &Credential) -> String {
    let status = tokio::time::timeout(
        Duration::from_secs(2),
        call_host(root, operator, "host.status", json!({})),
    )
    .await;
    let Ok(Ok(status)) = status else {
        return "supervisor_health=unobserved".to_owned();
    };
    let health = &status["host_lifecycle"]["optional_workers"]["module-supervisor"];
    let state = health["state"]
        .as_str()
        .filter(|state| {
            !state.is_empty()
                && state.len() <= 64
                && state.bytes().all(|b| b.is_ascii_lowercase() || b == b'_')
        })
        .unwrap_or("unobserved");
    let code = health["last_error_code"]
        .as_str()
        .filter(|code| fixture_diagnostic_code(code))
        .unwrap_or("UNOBSERVED");
    format!("supervisor_state={state},code={code}")
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
        Err(_) => {
            let phase = fs::read(evidence_path.with_file_name("adapter-startup-diagnostic.json"))
                .ok()
                .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
                .unwrap_or(Value::Null);
            json!({"fixture_failure":"APP320_EXTERNAL_COMPOSITION_FAILED","phase":phase["phase"],"code":phase["code"]})
        }
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

pub(super) async fn run_app299_case() -> std::result::Result<(), String> {
    run_connected_case().await
}

async fn run_connected_case() -> FixtureResult<()> {
    let scratch = unique_directory("eliot-app320-owned-connected")?;
    private_owned_directory(&scratch).map_err(display)?;
    let host_data_dir = scratch.join("host");
    let repository = scratch.join("repository");
    let workspace_root = scratch.join("workspaces");
    let hooks = scratch.join("empty-hooks");
    for directory in [&host_data_dir, &repository, &workspace_root, &hooks] {
        fs::create_dir(directory).map_err(display)?;
        // These are fresh fixture directories, not operator-configured roots.
        // Elevated Windows tokens can default their owner to Administrators;
        // a private DACL alone does not establish the exact-user owner that
        // the unchanged workspace validator requires.
        private_owned_directory(directory).map_err(display)?;
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

    let bun_executable = configured_bun()?;
    let install_root = scratch.join("native-install");
    let _ = write_installed_closure(&install_root, &test_server_source())?;
    let host_executable =
        install_root.join(format!("swarm-kernel-host{}", std::env::consts::EXE_SUFFIX));
    let supervisor = configured_binary("ELIOT_OPENCODE_TEST_SUPERVISOR")?;
    let owner_helper = configured_binary("ELIOT_OPENCODE_TEST_OWNER_HELPER")?;
    fs::copy(
        &supervisor,
        install_root.join(format!("swarm-supervisor{}", std::env::consts::EXE_SUFFIX)),
    )
    .map_err(display)?;
    let native_base = scratch.join("native-state");
    fs::create_dir(&native_base).map_err(display)?;
    private_owned_directory(&native_base).map_err(display)?;
    let native_connection_file = native_base.join("connection.json");
    let evidence_file = scratch.join("case-evidence.json");
    let case_file = scratch.join("case.json");

    fs::write(
        &case_file,
        serde_json::to_vec(&json!({
            "evidence_file":evidence_file,
            "admission_file":scratch.join("admission-evidence.json"),
            "execute_file":scratch.join("execute-after-public-readback"),
            "workspace_root":workspace_root,
            "native_base":native_base.clone()
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
        native_options: json!({
            "service_id":format!("app299-{}", uuid::Uuid::new_v4().simple()),
            "connection_file":native_connection_file,
            "directory":repository,
            "model":{
                "id":"fixture-model",
                "providerID":"openai",
                "variant":"fixture-variant"
            }
        }),
        workspace_option: Some("directory".to_owned()),
        admission_policy: None,
        owned_service: None,
    });
    config.module_supervisor.enabled = true;
    config.module_supervisor.install_root = Some(install_root.clone());
    config.module_supervisor.descriptor_files = vec![descriptor_file];
    config.module_supervisor.owner_helper_sha256 = Some(sha256_file(&owner_helper)?);
    config.module_supervisor.owner_helper = Some(owner_helper);
    let config_value = serde_json::to_value(&config).map_err(display)?;
    let config_file = scratch.join("host-config.json");
    fs::write(
        &config_file,
        serde_json::to_vec(&config_value).map_err(display)?,
    )
    .map_err(display)?;
    let mut external_native = spawn_external_native(
        &bun_executable,
        &install_root.join("resources/modules/opencode/serve.mjs"),
        &native_base,
        &native_connection_file,
        &repository,
    )
    .await?;
    let mut host = match tokio::process::Command::new(&host_executable)
        .args(["--exact", HOST_CHILD, "--ignored", "--nocapture"])
        .env("ELIOT_APP320_HOST_CONFIG", &config_file)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(false)
        .spawn()
    {
        Ok(host) => host,
        Err(error) => {
            let _ = stop_external_native(&mut external_native).await;
            return Err(display(error));
        }
    };
    let host_stdin = match host.stdin.take() {
        Some(stdin) => stdin,
        None => {
            let _ = host.kill().await;
            let _ = host.wait().await;
            let _ = stop_external_native(&mut external_native).await;
            return Err("host child stdin is unavailable".to_owned());
        }
    };
    let mut app299_client = None;
    let execution = async {
        let operator = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                if let Some(status) = host.try_wait().map_err(display)? {
                    return Err(format!("public host startup exited before IPC readiness: {status}; {}", startup_diagnostic(&scratch)));
                }
                if let Ok(credential) = load_credential(&host_data_dir.join("operator.json"))
                    && call_host(&host_data_dir, &credential, "module.catalog.get", json!({})).await.is_ok()
                {
                    break Ok(credential);
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
        }).await;
        let catalog = match catalog {
            Ok(result) => result?,
            Err(_) => return Err(format!("real supervisor descriptor registration was not available; {}", supervisor_diagnostic(&host_data_dir, &operator).await)),
        };
        if !catalog["catalog_revision"].is_number() {
            return Err("actual catalog omitted its revision".to_owned());
        }
        let (task, launch_id, launch_readback, admission) = {
                // Use the supported launcher producer to establish exact Task/Attempt/lease provenance.
                // The first child admission is the public launcher IPC operation, not an MCP agent_open.
                let task = call_host(&host_data_dir, &operator, "task.create", json!({
                    "client_request_id":uuid::Uuid::new_v4().to_string(),"project_id":PROJECT_ID,
                    "spec":{"objective":"Create and acknowledge a native session",
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
                    return Err("real APP299 public owned launch preview reported a blocker".to_owned());
                }
                request["plan_digest"] = preview["plan_digest"].clone();
                request["client_request_id"] = json!(uuid::Uuid::new_v4().to_string());
                let launch = call_host(&host_data_dir, &operator, "swarm.launch", request).await?;
                let launch_id = launch["operation_id"]
                    .as_str()
                    .ok_or_else(|| "public APP299 launch omitted its operation ID".to_owned())?
                    .to_owned();
                // Wait until the production adapter has received the real launch-created child via module.next.
                let admission = tokio::time::timeout(Duration::from_secs(90), async {
                    loop {
                        if let Ok(bytes) = fs::read(scratch.join("admission-evidence.json"))
                            && let Ok(value) = serde_json::from_slice::<Value>(&bytes)
                        {
                            break Ok::<_, String>(value);
                        }
                        if evidence_file.is_file() {
                            let evidence: Value = serde_json::from_slice(&fs::read(&evidence_file).map_err(display)?).map_err(display)?;
                            return Err(format!("APP299 adapter failed before launch-child readback; phase={},code={}", evidence["phase"].as_str().unwrap_or("unknown"), evidence["code"].as_str().unwrap_or("UNOBSERVED")));
                        }
                        if host.try_wait().map_err(display)?.is_some() {
                            return Err("host exited before APP299 launch-child admission".to_owned());
                        }
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                })
                .await
                .map_err(|_| "production supervisor did not deliver the launcher-created APP299 child".to_owned())??;
                let launch_readback = call_host(
                    &host_data_dir,
                    &operator,
                    "operation.get",
                    json!({"operation_id":launch_id}),
                )
                .await?;
                let operation_id = admission["operation_id"]
                    .as_str()
                    .ok_or_else(|| "module.next admission omitted the child Operation ID".to_owned())?
                    .to_owned();
                let command_readback = call_host(
                    &host_data_dir,
                    &operator,
                    "operation.get",
                    json!({"operation_id":operation_id}),
                )
                .await?;
                let child_state = command_readback["state"].as_str().unwrap_or_default();
                if launch_readback["method"] != json!("swarm.launch")
                    || launch_readback["operation_id"] != json!(launch_id)
                    || launch_readback["state"] != json!("queued")
                    || launch_readback["task_id"] != task["task_id"]
                    || !launch_readback["attempt_id"].as_str().is_some_and(|id| !id.is_empty())
                    || launch_readback["binding_id"] != admission["binding_id"]
                    || launch_readback["binding_generation"] != admission["generation"]
                    || command_readback["operation_id"] != json!(operation_id)
                    || command_readback["method"] != json!("agent.open")
                    || command_readback["task_id"] != task["task_id"]
                    || command_readback["attempt_id"] != launch_readback["attempt_id"]
                    || !command_readback["attempt_id"].as_str().is_some_and(|id| !id.is_empty())
                    || command_readback["prerequisite_operation_id"] != json!(launch_id)
                    || command_readback["binding_id"] != admission["binding_id"]
                    || command_readback["binding_generation"] != admission["generation"]
                    || !["queued", "sending"].contains(&child_state)
                {
                    return Err("public launch and module.next do not identify the exact child Task/Attempt/parent/binding".to_owned());
                }
                let original_request = super::app299_connected_consumer::assert_retained_agent_open(
                    &host_data_dir,
                    &operation_id,
                    &launch_id,
                    ROUTE_ALIAS,
                    &workspace_root,
                )?;
                let mut client = super::app299_connected_consumer::PublicTasksClient::connect(
                    host_data_dir.clone(),
                    operator.clone(),
                )
                .await?;
                let public_task_id = client
                    .agent_open_duplicate(&original_request, &operation_id)
                    .await?;
                if public_task_id != operation_id {
                    return Err("public MCP duplicates did not replay the exact launcher child Operation".to_owned());
                }
                let working = client.task_get(&operation_id).await?;
                if working["taskId"] != json!(operation_id)
                    || working["status"] != json!("working")
                {
                    return Err("MCP tasks/get did not read the launcher child as working before execution".to_owned());
                }
                fs::write(scratch.join("execute-after-public-readback"), b"continue
")
                    .map_err(display)?;
                app299_client = Some(client);
                (
                    task,
                    launch_id,
                    launch_readback,
                    admission,
                )
        };
        let mut evidence = tokio::time::timeout(Duration::from_secs(180), async {
            loop {
                if let Ok(bytes) = fs::read(&evidence_file)
                    && let Ok(evidence) = serde_json::from_slice::<Value>(&bytes)
                {
                    break Ok::<_, String>(evidence);
                }
                if host.try_wait().map_err(display)?.is_some() { return Err("host exited before owned worker evidence".to_owned()); }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }).await.map_err(|_| "owned worker did not publish bounded connected evidence".to_owned())??;
        if evidence.get("fixture_failure").is_some() { return Err("actual owned composition failed; private scratch retained".to_owned()); }
        let operation_id = evidence["operation_id"]
            .as_str()
            .ok_or_else(|| "worker evidence omitted its actual command".to_owned())?;
        let operation = call_host(
            &host_data_dir,
            &operator,
            "operation.get",
            json!({"operation_id":operation_id}),
        )
        .await?;
        let expected_operation_id = admission["operation_id"].as_str().ok_or_else(|| "admission omitted exact command identity".to_owned())?;
        if operation_id != expected_operation_id
            || operation["method"] != json!("agent.open")
            || operation["operation_id"] != json!(expected_operation_id)
            || operation["binding_id"] != admission["binding_id"]
            || operation["binding_generation"] != admission["generation"]
            || operation["state"] != json!("settled")
            || operation["result"]["outcome"] != json!("applied")
            || operation["task_id"] != task["task_id"]
            || operation["attempt_id"] != launch_readback["attempt_id"]
            || operation["prerequisite_operation_id"] != json!(launch_id)
            || admission["native_pid"] != evidence["native_pid"]
            || evidence["outcome"]["outcome"] != json!("applied")
            || evidence["outcome"]["details"]["completion_condition"]
                != json!("native_session_created")
            || evidence["outcome"]["details"]["durable_origin"]
                != json!("exact_session_created_event")
        {
            return Err("public Operation, adapter outcome and module.next command do not share the APP299 identity".to_owned());
        }
        let completed = app299_client
            .as_mut()
            .ok_or_else(|| "public Tasks client was not retained through consumption".to_owned())?
            .task_get(expected_operation_id)
            .await?;
        if completed["taskId"] != json!(expected_operation_id)
            || completed["status"] != json!("completed")
        {
            return Err("MCP tasks/get did not project the acknowledged Operation as completed".to_owned());
        }
        let client = app299_client
            .take()
            .ok_or_else(|| "public Tasks client was not retained through completion".to_owned())?;
        client.close().await;
        evidence["tasks_get_readback"] = completed;
        if operation["result"].get("details").is_some()
        {
            return Err("public operation.get exposed private outcome details".to_owned());
        }
        evidence["operation_readback"] = operation;
        Ok((operator, evidence))
    }.await;
    if let Some(client) = app299_client.take() {
        client.close().await;
    }
    // EOF is local to this host child; it invokes production graceful shutdown.
    drop(host_stdin);
    let mut stopped = tokio::time::timeout(Duration::from_secs(60), host.wait()).await;
    let graceful_wait = matches!(&stopped, Ok(Ok(_)));
    if !graceful_wait {
        // Retain the original failure and stop only this fixture's exact
        // child before withdrawing its fake native dependency.
        let _ = host.kill().await;
        stopped = tokio::time::timeout(Duration::from_secs(15), host.wait()).await;
    }
    let host_gone = matches!(&stopped, Ok(Ok(_)));
    let host_ok = matches!(&stopped, Ok(Ok(status)) if status.success());
    let mut errors = Vec::new();
    if !host_ok || !graceful_wait {
        let (state, code) = match &stopped {
            Ok(Ok(status)) => ("exited", status.code()),
            Ok(Err(_)) => ("wait_failed", None),
            Err(_) => ("deadline_expired", None),
        };
        errors.push(format!(
            "public host child did not confirm successful graceful shutdown; state={state},exit_code={code:?}; {}",
            startup_diagnostic(&scratch)
        ));
    }
    let native_alive = external_native.child.try_wait().map_err(display)?.is_none()
        && !endpoint_listener_gone(&external_native.url).await
        && read_external_registration(&external_native.registration_file).is_ok_and(
            |registration| {
                registration.pid == external_native.pid
                    && registration.url == external_native.url
                    && registration.version == external_native.version
            },
        );
    if !native_alive {
        errors.push("controller stopped or replaced the external native service".to_owned());
    }
    let native_stop = if host_gone {
        stop_external_native(&mut external_native).await
    } else {
        Err("host departure remains unknown; external dependency retained".to_owned())
    };
    let mut case_ok = false;
    match execution {
        Ok((operator, mut evidence)) => {
            let ipc_gone = swarm_client::ModuleLink::connect(
                &host_data_dir,
                &operator,
                &swarm_client::IpcConfig::default(),
            )
            .await
            .is_err();
            if !ipc_gone {
                errors.push("host IPC listener still accepts authenticated connections".to_owned());
            }
            let observation: Value = serde_json::from_slice(
                &fs::read(&external_native.observation_file).map_err(display)?,
            )
            .map_err(display)?;
            let posted = &observation["root_create_request"];
            let expected_root = &evidence["outcome"]["native_root_id"];
            let posts = observation["route_identities"].as_array().map(|routes| {
                routes
                    .iter()
                    .filter(|route| **route == json!("POST /api/session"))
                    .count()
            });
            if observation["root_create_posts"] != 1
                || posts != Some(1)
                || posted["id"] != *expected_root
                || observation["root_session_created_id"] != *expected_root
                || posted["metadata"]["eliot"]["operation"] != evidence["operation_id"]
                || posted["metadata"]["eliot"]["binding"] != evidence["binding_id"]
                || posted["metadata"]["eliot"]["generation"] != evidence["generation"]
                || posted["location"]["directory"] != evidence["workspace_directory"]
                || posted["model"]
                    != json!({"id":"fixture-model","providerID":"openai","variant":"fixture-variant"})
                || posted
                    .get("parentID")
                    .is_some_and(|parent| !parent.is_null())
                || posted.get("fork").is_some_and(|fork| !fork.is_null())
            {
                errors.push(
                    "native POST count/session identity differs from the retained Operation"
                        .to_owned(),
                );
            }
            if let Err(error) = &native_stop {
                errors.push(error.clone());
            }
            let listener_gone = endpoint_listener_gone(&external_native.url).await;
            case_ok = native_alive && native_stop.is_ok() && listener_gone && ipc_gone;
            evidence["external_process_alive_after_adapter_stop"] = json!(native_alive);
            evidence["native_stop_confirmed"] = json!(native_stop.is_ok());
            evidence["native_listener_gone"] = json!(listener_gone);
            evidence["native_http_observation"] = observation;
            evidence["root_posts"] = json!(1);
            evidence["connection_absent"] = json!(native_stop.is_ok());
            evidence["ipc_listener_gone"] = json!(ipc_gone);
            fs::write(
                &evidence_file,
                serde_json::to_vec(&evidence).map_err(display)?,
            )
            .map_err(display)?;
        }
        Err(error) => {
            errors.push(error);
            if let Err(stop_error) = native_stop {
                errors.push(stop_error);
            }
        }
    }
    // Never remove custody/receipts after an unknown host/native teardown.
    if host_gone && case_ok && errors.is_empty() {
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
    let phase_file = PathBuf::from(
        case["evidence_file"]
            .as_str()
            .ok_or_else(|| "case omitted evidence path".to_owned())?,
    )
    .with_file_name("adapter-startup-diagnostic.json");
    let stage = |phase: &str, code: Option<&str>| {
        let safe_code = code.filter(|code| fixture_diagnostic_code(code));
        let _ = fs::write(
            &phase_file,
            serde_json::to_vec(&json!({"phase":phase,"code":safe_code})).unwrap(),
        );
    };
    stage("bootstrap", None);
    let scenario = "app299-public-tasks-exact-root-session";
    let OwnedBootstrap {
        config: adapter_config,
        credential,
        worker,
        contract: claim,
    } = OwnedBootstrap::from_config_path(host_path).map_err(|error| {
        stage("bootstrap", Some(&error.code));
        display(error)
    })?;
    stage("route_scope", None);
    let native_options = adapter_config.native_options.clone();
    if adapter_config.owned_native.is_some() {
        return Err(
            "external attach demand unexpectedly carried fresh native ownership".to_owned(),
        );
    }
    let allowed_workspace_root = fs::canonicalize(
        case["workspace_root"]
            .as_str()
            .ok_or_else(|| "case omitted workspace root".to_owned())?,
    )
    .map_err(display)?;
    let canonical_native_directory =
        fs::canonicalize(&native_options.directory).map_err(display)?;
    let native_base = PathBuf::from(
        case["native_base"]
            .as_str()
            .ok_or_else(|| "case omitted native base".to_owned())?,
    );
    let native_registration = read_external_registration(&native_options.connection_file)?;
    if !canonical_native_directory.starts_with(&allowed_workspace_root)
        || canonical_native_directory == allowed_workspace_root
        || native_options.connection_file != native_base.join("connection.json")
        || native_options.model.id != "fixture-model"
        || native_options.model.provider_id != "openai"
        || native_options.model.variant != "fixture-variant"
        || native_registration.version != FAKE_NATIVE_VERSION
    {
        return Err(
            "protected demand did not retain its exact admitted external workspace/identity scope"
                .to_owned(),
        );
    }
    let binding_id = adapter_config.binding_id.clone();
    let generation = adapter_config.generation;
    let journal_dir = adapter_config.state_dir.clone();
    let route_sha256 =
        digest_json(&serde_json::to_value(&native_options).map_err(display)?).map_err(display)?;
    stage("journal", None);
    let journal = Journal::open(
        &journal_dir,
        &binding_id,
        generation,
        &native_options.scope_key(),
        &route_sha256,
    )
    .map_err(display)?;
    // The command handler retains this parameter for its external-attach path.
    // None is the explicit no-spawn/no-stop ownership fence.
    let mut native_owner = NativeOwnerController::new(None);
    let observer = Arc::new(Mutex::new(Vec::<Value>::new()));
    let boot_id = worker.boot_id.clone();
    let owner = &worker.owner_record;
    let (native_service, native_observation) = NativeClient::connect(&native_options)
        .await
        .map_err(display)?;
    native_service
        .probe_route(&native_options)
        .await
        .map_err(display)?;
    if native_observation.pid != native_registration.pid
        || native_observation.version != native_registration.version
    {
        return Err("native service differs from its registration before module hello".to_owned());
    }
    let session = HostSession {
        config: &adapter_config,
        credential: &credential,
        journal: &journal,
        claim: &claim,
        boot_id: boot_id.clone(),
        hello_base: json!({"boot_id":boot_id,"module_artifact_id":ARTIFACT_ID,"native_ready":true,
            "managed_owner":{"token":owner["token"],"process":owner["process"]}}),
        root_hint: Mutex::new(None),
        ipc_link: AsyncMutex::new(None),
        outcome_observer: Some(Arc::clone(&observer)),
        retention_retry_pending: AtomicBool::new(false),
    };
    async {
        stage("module_hello", None);
        session.hello_retry().await.map_err(|error| {
            stage("module_hello", Some(&error.code));
            display(error)
        })?;
        stage("module_next", None);
        let command: RuntimeCommand = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let next = session
                    .call("module.next", json!({}))
                    .await
                    .map_err(display)?;
                if let Some(command) = next.get("command").filter(|value| !value.is_null()) {
                    break serde_json::from_value(command.clone()).map_err(display);
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .map_err(|_| "actual module.next did not supply the admitted command".to_owned())??;
        if command.method != "agent.open"
            || command.binding_id != binding_id
            || command.generation != generation
            || command.route["alias"] != ROUTE_ALIAS
            || command.route["workspace_option"] != "directory"
            || command.route["native_options"]["directory"] != json!(native_options.directory)
        {
            return Err(
                "actual module.next command differs from protected owned demand".to_owned(),
            );
        }
        verify_command_scope(&adapter_config, &command, &claim).map_err(display)?;
        stage("command_scope", None);
        let operation_id = command.operation_id.clone();
        let admission_path = PathBuf::from(
            case["admission_file"]
                .as_str()
                .ok_or_else(|| "case omitted admission handshake path".to_owned())?,
        );
        let execute_path = PathBuf::from(
            case["execute_file"]
                .as_str()
                .ok_or_else(|| "case omitted execute handshake path".to_owned())?,
        );
        fs::write(
            admission_path,
            serde_json::to_vec(&json!({
                "operation_id":operation_id,"binding_id":binding_id,"generation":generation,
                "native_pid":native_registration.pid,
                "native_url":native_registration.url,
                "native_version":native_registration.version,
                "workspace_directory":native_options.directory
            }))
            .map_err(display)?,
        )
        .map_err(display)?;
        tokio::time::timeout(Duration::from_secs(30), async {
            while !execute_path.is_file() {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .map_err(|_| "parent did not confirm actual public admission readback".to_owned())?;
        stage("native_command", None);
        handle_command(&session, &command, &mut native_owner)
            .await
            .map_err(display)?;

        let history = journal.load(&operation_id).map_err(display)?;
        let outcome = history
            .outcome
            .clone()
            .ok_or_else(|| "Journal did not retain the RuntimeOutcome".to_owned())?;
        let expected_root = root_id(&binding_id, generation);
        let current_registration = read_external_registration(&native_options.connection_file)?;
        if current_registration.pid != native_registration.pid
            || current_registration.url != native_registration.url
            || current_registration.version != native_registration.version
        {
            return Err(format!(
                "{scenario}: external native registration identity changed during the command"
            ));
        }
        if outcome["outcome"] != "applied"
            || outcome["native_root_id"] != expected_root
            || outcome["details"]["completion_condition"] != "native_session_created"
            || outcome["details"]["durable_origin"] != "exact_session_created_event"
            || outcome["details"]["native_replay"] != false
        {
            return Err(format!(
                "{scenario}: Journal outcome changed the exact root-session proof"
            ));
        }

        let observed_outcomes = observer
            .lock()
            .map_err(|_| "module.outcome observer mutex was poisoned".to_owned())?
            .clone();
        if observed_outcomes.len() != 1 || observed_outcomes[0] != outcome {
            return Err("production HostSession did not send the exact outcome once".to_owned());
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
            native_pid: native_registration.pid,
            native_url: native_registration.url,
            native_version: native_registration.version,
            outcome,
            operation_readback: Value::Null,
            observed_outcomes,
            acknowledged_sha256,
            pending_count,
            root_posts: 0,
            connection_absent: false,
            native_listener_gone: false,
            external_process_alive_after_adapter_stop: false,
            native_stop_confirmed: false,
            ipc_listener_gone: false,
        })
    }
    .await
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

fn test_server_source() -> String {
    r#"import { createServer } from "node:http";
import { mkdir, unlink, writeFile } from "node:fs/promises";
import path from "node:path";
const arg = (name) => { const i = process.argv.indexOf(name); return i < 0 ? undefined : process.argv[i + 1]; };
if (Bun.version !== "1.4.0") throw new Error("fixture requires the selected Bun runtime");
const stateRoot = path.resolve(arg("--state-root"));
const connectionFile = path.resolve(arg("--registration-file"));
const workspace = path.resolve(arg("--workspace-directory"));
const password = process.env.ELIOT_APP299_FAKE_HTTP_PASSWORD;
const observationFile = path.join(stateRoot, "fixture-observation.json");
const stopFile = path.join(stateRoot, "stop.json");
await mkdir(path.join(stateRoot, "tmp"), { recursive: true });
let endpoint = "";
let rootCreatePosts = 0;
let rootCreateRequest = null;
let rootSession = null;
const routes = [];
const expectedAuthorization = `Basic ${Buffer.from(`opencode:${password}`).toString("base64")}`;
async function observe() {
  await writeFile(observationFile, JSON.stringify({
    root_create_posts: rootCreatePosts, root_create_request: rootCreateRequest,
    root_session_created_id: rootSession?.id ?? null, route_identities: routes, endpoint
  }), { mode: 0o600 });
}
const server = createServer(async (request, response) => {
  try {
    const url = new URL(request.url, endpoint);
    routes.push(`${request.method} ${url.pathname}`);
    if (request.headers.authorization !== expectedAuthorization) { response.writeHead(401).end(); return; }
    const directory = url.searchParams.get("location[directory]") || workspace;
    const send = (status, value) => { response.writeHead(status, { "content-type": "application/json" }); response.end(JSON.stringify(value)); };
    if (request.method === "GET" && url.pathname === "/api/info") { send(200, { version: "2.0.7", pid: process.pid, urls: [endpoint], paths: { tmp: path.join(stateRoot, "tmp") } }); return; }
    if (request.method === "GET" && url.pathname === "/api/location") { send(200, { directory, project: { id: "app299-fixture-project" } }); return; }
    if (request.method === "GET" && url.pathname === "/api/model") { send(200, { location: { directory }, data: [{ id: "fixture-model", providerID: "openai", enabled: true, variants: [{ id: "fixture-variant" }] }] }); return; }
    if (request.method === "GET" && url.pathname.startsWith("/api/experimental/session/") && url.pathname.endsWith("/log")) {
      const root = url.pathname.split("/")[4];
      if (!rootSession || root !== rootSession.id || url.searchParams.get("follow") !== "false") { send(404, {}); return; }
      const event = { id: "evt_app299_created", type: "session.created", created: 1700000000,
        durable: { aggregateID: root, version: 1, seq: 0 },
        data: { sessionID: root, projectID: "global", location: rootSession.location, slug: "app299-fixture", version: "2.0.7", metadata: rootSession.metadata, model: rootSession.model } };
      const after = url.searchParams.get("after");
      response.writeHead(200, { "content-type": "text/event-stream" });
      response.end((after === null ? `data: ${JSON.stringify(event)}\n\n` : "") + `data: ${JSON.stringify({ type: "log.synced", aggregateID: root, seq: 0 })}\n\n`);
      return;
    }
    if (request.method === "GET" && rootSession && url.pathname === `/api/session/${rootSession.id}`) { send(200, { data: rootSession }); return; }
    if (request.method === "POST" && url.pathname === "/api/session") {
      const chunks = []; for await (const chunk of request) chunks.push(chunk);
      rootCreateRequest = JSON.parse(Buffer.concat(chunks).toString("utf8")); rootCreatePosts++;
      rootSession = { id: rootCreateRequest.id, parentID: null, fork: null, location: rootCreateRequest.location, model: rootCreateRequest.model, metadata: rootCreateRequest.metadata };
      await observe(); send(200, { data: rootSession }); return;
    }
    for await (const _chunk of request) {}
    send(404, {});
  } catch { if (!response.headersSent) response.writeHead(500); response.end(); }
});
await new Promise((resolve, reject) => { server.once("error", reject); server.listen(0, "127.0.0.1", resolve); });
endpoint = `http://127.0.0.1:${server.address().port}`;
await writeFile(connectionFile, JSON.stringify({ url: endpoint, pid: process.pid, version: "2.0.7", password }), { mode: 0o600 });
let stopping = false;
process.stdin.once("end", () => {
  if (stopping) return; stopping = true;
  server.close(async () => {
    try { await observe(); await unlink(connectionFile); await writeFile(stopFile, JSON.stringify({ status: "stopped", pid: process.pid, connection_absent: true, process_exit_code: 0, endpoint }), { mode: 0o600 }); }
    catch { process.exitCode = 1; }
  });
  server.closeAllConnections();
});
process.stdin.resume();
"#.to_owned()
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
