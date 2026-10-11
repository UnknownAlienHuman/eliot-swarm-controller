//! Shared Git hook setup and local-operation controller used by the CLI and its connected tests.

use crate::{
    config::Config,
    error::{Error, Result},
    ipc,
    model::{self, Credential},
};
use serde_json::{Value, json};
use std::path::Path;
/// Dispatch one authenticated local hook operation and validate the typed Store result.
pub async fn hook_local_operation(
    config: &Config,
    operator: &Credential,
    project_id: &str,
    operation: crate::hooks::git::HookLocalOperation,
) -> Result<crate::hooks::git::HookLocalResult> {
    use crate::hooks::git::{HookLocalRequest, HookLocalResult};

    let value = ipc::call(
        &config.storage.data_dir,
        operator,
        "hook.local.operation",
        serde_json::to_value(HookLocalRequest {
            project_id: project_id.to_owned(),
            operation,
        })?,
        &config.ipc,
    )
    .await?;
    let result: HookLocalResult = serde_json::from_value(value).map_err(|_| {
        Error::new(
            "HOOK_LOCAL_RESPONSE_INVALID",
            "Store returned an invalid local hook operation result",
        )
    })?;
    hook_local_response(result)
}

fn hook_local_response(
    result: crate::hooks::git::HookLocalResult,
) -> Result<crate::hooks::git::HookLocalResult> {
    use crate::hooks::git::HookLocalResult;

    match result {
        HookLocalResult::CleanupPending {
            operation,
            cause_code,
        } => Err(Error::new(
            "HOOK_GIT_CLEANUP_PENDING",
            format!(
                "hook {operation} is incomplete; exact Git helper custody remains in Store until process-family departure{}",
                cause_code
                    .map(|code| format!(" ({code})"))
                    .unwrap_or_default()
            ),
        )),
        other => Ok(other),
    }
}

fn setup_effect_requires_readback(error: &Error) -> bool {
    matches!(
        error.code.as_str(),
        "HOOK_GIT_CLEANUP_PENDING"
            | "OUTCOME_UNKNOWN"
            | "HOOK_LOCAL_RESPONSE_INVALID"
            | "QUALIFICATION_REPLY_DROPPED"
    )
}

fn unresolved_hook_setup(error: Error, source_id: &str, request_id: &str) -> Error {
    Error::new(
        error.code.clone(),
        format!(
            "hook setup outcome is unresolved; retain source/request {source_id}/{request_id} and reconcile exact local readback/cleanup before revocation or rollback"
        ),
    )
    .with_secondary_error(error)
}

fn hook_source_public_projection(source: &Value) -> Result<Value> {
    let object = source.as_object().ok_or_else(|| {
        Error::new(
            "HOOK_SETUP_RESPONSE_INVALID",
            "hook setup public source is invalid",
        )
    })?;
    let mut public = serde_json::Map::new();
    for name in [
        "source_id",
        "project_id",
        "event",
        "phase",
        "veto",
        "canonical_repository",
        "registration_id",
        "registration_generation",
        "revision",
        "enabled",
    ] {
        let value = object.get(name).ok_or_else(|| {
            Error::new(
                "HOOK_SETUP_RESPONSE_INVALID",
                "hook setup public source is incomplete",
            )
        })?;
        public.insert(name.to_owned(), value.clone());
    }
    if public["source_id"].as_str().is_none_or(str::is_empty)
        || public["project_id"].as_str().is_none_or(str::is_empty)
        || public["canonical_repository"]
            .as_str()
            .is_none_or(str::is_empty)
        || public["registration_id"].as_str().is_none_or(str::is_empty)
        || public["registration_generation"]
            .as_i64()
            .is_none_or(|generation| generation <= 0)
        || public["revision"]
            .as_i64()
            .is_none_or(|revision| revision <= 0)
        || public["enabled"] != true
    {
        return Err(Error::new(
            "HOOK_SETUP_RESPONSE_INVALID",
            "hook setup public source fields are invalid",
        ));
    }
    Ok(Value::Object(public))
}

/// Send a revision-fenced source revocation request to Store.
pub async fn revoke_hook_source(
    config: &Config,
    operator: &Credential,
    source_id: &str,
    revision: i64,
    request_id: Option<&str>,
) -> Result<Value> {
    let request_id = request_id.map(str::to_owned).unwrap_or_else(model::new_id);
    ipc::call(
        &config.storage.data_dir,
        operator,
        "hook.source.revoke",
        json!({
            "client_request_id":request_id,
            "source_id":source_id,
            "expected_revision":revision
        }),
        &config.ipc,
    )
    .await
}

fn sanitized_hook_install_error(code: &str) -> Error {
    Error::new(
        code.to_owned(),
        "hook installation readback did not complete",
    )
}

async fn rollback_hook_setup(
    config: &Config,
    operator: &Credential,
    project_id: &str,
    source_id: &str,
    client_request_id: &str,
    revision: i64,
) -> Result<(bool, bool)> {
    let source_revoked = match revoke_hook_source(config, operator, source_id, revision, None).await
    {
        Ok(_) => true,
        Err(error) if error.code == "HOOK_GIT_CLEANUP_PENDING" => return Err(error),
        Err(_) => false,
    };
    if !source_revoked {
        return Ok((false, false));
    }
    use crate::hooks::git::{HookLocalOperation, HookLocalResult};
    let local = hook_local_operation(
        config,
        operator,
        project_id,
        HookLocalOperation::RollbackSetup {
            source_id: source_id.to_owned(),
            client_request_id: client_request_id.to_owned(),
        },
    )
    .await;
    match local {
        Ok(HookLocalResult::RolledBack {
            restored,
            pending_removed,
        }) => Ok((source_revoked, restored && pending_removed)),
        Err(error) if error.code == "HOOK_GIT_CLEANUP_PENDING" => Err(error),
        _ => Ok((source_revoked, false)),
    }
}

/// Run hook setup while retaining unresolved effects for exact readback reconciliation.
pub async fn hook_setup(
    config: &Config,
    operator: &Credential,
    request_id: &Option<String>,
    project_id: &str,
) -> Result<()> {
    use crate::hooks::git::{HookLocalOperation, HookLocalResult, prepare_source_credential};

    let preview =
        match hook_local_operation(config, operator, project_id, HookLocalOperation::Preview)
            .await?
        {
            HookLocalResult::Preview { preview } => preview,
            _ => {
                return Err(Error::new(
                    "HOOK_LOCAL_RESPONSE_INVALID",
                    "Store did not return a hook preview",
                ));
            }
        };
    let git_dir = preview["git_directory"].as_str().ok_or_else(|| {
        Error::new(
            "HOOK_LOCAL_RESPONSE_INVALID",
            "Store hook preview omitted the configured Git directory",
        )
    })?;
    let prepared =
        prepare_source_credential(Path::new(git_dir), project_id, request_id.as_deref())?;
    eprintln!(
        "hook_setup_request_id={} source_id={}",
        serde_json::to_string(&prepared.client_request_id)?,
        serde_json::to_string(&prepared.source_id)?
    );
    let issued = match ipc::call(
        &config.storage.data_dir,
        operator,
        "hook.source.setup",
        json!({
            "client_request_id":&prepared.client_request_id,
            "project_id":&prepared.project_id,
            "source_id":&prepared.source_id,
            "credential":serde_json::to_value(&prepared.credential)?
        }),
        &config.ipc,
    )
    .await
    {
        Ok(value) => value,
        Err(error) => {
            // A failure of this connection does not establish that an earlier
            // attempt with the same pending identity failed to commit. Retain
            // its credential until exact source readback/revocation resolves it.
            return Err(Error::new(
                error.code.clone(),
                format!(
                    "hook setup failed with {}; read back or retry the retained private source/request {}/{} before cleanup",
                    error.code, prepared.source_id, prepared.client_request_id
                ),
            ));
        }
    };

    let source = issued.get("source").ok_or_else(|| {
        Error::new(
            "HOOK_SETUP_RESPONSE_INVALID",
            format!(
                "hook setup returned no public source; retry retained source/request {}/{}",
                prepared.source_id, prepared.client_request_id
            ),
        )
    })?;
    let raw_revision = source
        .get("revision")
        .and_then(Value::as_i64)
        .unwrap_or_default();
    let public_source = match hook_source_public_projection(source) {
        Ok(source) => source,
        Err(error) => {
            let revision = if raw_revision > 0 { raw_revision } else { 1 };
            let (source_revoked, local_restored) = rollback_hook_setup(
                config,
                operator,
                project_id,
                &prepared.source_id,
                &prepared.client_request_id,
                revision,
            )
            .await?;
            if !source_revoked || !local_restored {
                return Err(Error::new(
                    "HOOK_SETUP_ROLLBACK_INCOMPLETE",
                    format!(
                        "invalid setup readback; inspect source/request {}/{}",
                        prepared.source_id, prepared.client_request_id
                    ),
                ));
            }
            return Err(Error::new(
                error.code,
                "hook setup public readback was invalid",
            ));
        }
    };
    let source_id = public_source["source_id"].as_str().unwrap_or_default();
    let revision = public_source["revision"].as_i64().unwrap_or_default();
    let expected_repository =
        config.forge.projects.get(project_id).and_then(|project| {
            crate::forge::canonical_repository(&project.canonical_repository).ok()
        });
    let source_matches = source_id == prepared.source_id.as_str()
        && public_source["project_id"] == project_id
        && public_source["event"] == "git.post_commit"
        && public_source["phase"] == "post_commit"
        && public_source["veto"] == "none_after_commit"
        && expected_repository
            .as_deref()
            .is_some_and(|repository| public_source["canonical_repository"] == repository);
    if !source_matches {
        let rollback_revision = if revision > 0 { revision } else { 1 };
        let (source_revoked, local_restored) = rollback_hook_setup(
            config,
            operator,
            project_id,
            &prepared.source_id,
            &prepared.client_request_id,
            rollback_revision,
        )
        .await?;
        if !source_revoked || !local_restored {
            return Err(Error::new(
                "HOOK_SETUP_ROLLBACK_INCOMPLETE",
                format!(
                    "source metadata did not match; inspect source/request {}/{}",
                    prepared.source_id, prepared.client_request_id
                ),
            ));
        }
        return Err(Error::new(
            "HOOK_SETUP_SCOPE_MISMATCH",
            "issued hook source does not match the selected configured repository",
        ));
    }

    let installation = match hook_local_operation(
        config,
        operator,
        project_id,
        HookLocalOperation::Apply {
            source_id: source_id.to_owned(),
            credential: prepared.credential.clone(),
        },
    )
    .await
    {
        Ok(HookLocalResult::Installed { installation }) => installation,
        Ok(_) => {
            return Err(Error::new(
                "HOOK_LOCAL_RESPONSE_INVALID",
                "Store did not return hook installation readback",
            ));
        }
        Err(error) if setup_effect_requires_readback(&error) => {
            return Err(unresolved_hook_setup(
                error,
                &prepared.source_id,
                &prepared.client_request_id,
            ));
        }
        Err(error) => {
            let (source_revoked, local_restored) = rollback_hook_setup(
                config,
                operator,
                project_id,
                &prepared.source_id,
                &prepared.client_request_id,
                revision,
            )
            .await?;
            if !source_revoked || !local_restored {
                return Err(Error::new(
                    "HOOK_SETUP_ROLLBACK_INCOMPLETE",
                    format!(
                        "setup failed with {}; inspect source/request {}/{}",
                        error.code, prepared.source_id, prepared.client_request_id
                    ),
                ));
            }
            return Err(Error::new(
                error.code,
                "hook setup failed; its issued source and local installation were rolled back",
            ));
        }
    };

    let readback = match hook_local_operation(
        config,
        operator,
        project_id,
        HookLocalOperation::Readback {
            source_id: source_id.to_owned(),
        },
    )
    .await
    {
        Ok(HookLocalResult::Readback { readback })
            if readback.state == "installed" && readback.wrapper_matches =>
        {
            readback
        }
        result => {
            let failure_code = match result {
                Err(error) if setup_effect_requires_readback(&error) => {
                    return Err(unresolved_hook_setup(
                        error,
                        &prepared.source_id,
                        &prepared.client_request_id,
                    ));
                }
                Err(error) => error.code,
                Ok(_) => "HOOK_INSTALL_READBACK_FAILED".to_owned(),
            };
            let (source_revoked, local_restored) = rollback_hook_setup(
                config,
                operator,
                project_id,
                &prepared.source_id,
                &prepared.client_request_id,
                revision,
            )
            .await?;
            if !source_revoked || !local_restored {
                return Err(Error::new(
                    "HOOK_SETUP_ROLLBACK_INCOMPLETE",
                    format!(
                        "readback failed with {failure_code}; inspect source/request {}/{}",
                        prepared.source_id, prepared.client_request_id
                    ),
                ));
            }
            return Err(sanitized_hook_install_error(&failure_code));
        }
    };
    if installation.state != "installed" || !installation.wrapper_matches {
        let (source_revoked, local_restored) = rollback_hook_setup(
            config,
            operator,
            project_id,
            &prepared.source_id,
            &prepared.client_request_id,
            revision,
        )
        .await?;
        if !source_revoked || !local_restored {
            return Err(Error::new(
                "HOOK_SETUP_ROLLBACK_INCOMPLETE",
                format!(
                    "inspect source/request {}/{}",
                    prepared.source_id, prepared.client_request_id
                ),
            ));
        }
        return Err(sanitized_hook_install_error("HOOK_INSTALL_READBACK_FAILED"));
    }
    match hook_local_operation(
        config,
        operator,
        project_id,
        HookLocalOperation::CompleteSetup {
            source_id: prepared.source_id.clone(),
            client_request_id: prepared.client_request_id.clone(),
        },
    )
    .await
    {
        Ok(HookLocalResult::Completed { .. }) => {}
        Err(error) if error.code == "HOOK_GIT_CLEANUP_PENDING" => return Err(error),
        _ => {
            return Err(Error::new(
                "HOOK_SETUP_FINALIZATION_PENDING",
                format!(
                    "installation is active; retry source/request {}/{} to complete readback",
                    prepared.source_id, prepared.client_request_id
                ),
            ));
        }
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "source":public_source,
            "credential_file_written":true,
            "installation":readback
        }))?
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{hook_local_response, setup_effect_requires_readback, unresolved_hook_setup};
    use crate::{
        config::Config,
        error::Error,
        forge::ForgeProject,
        hooks::git::{HookLocalOperation, HookLocalResult, prepare_source_credential},
        host,
        ipc::{self, Stream},
        model,
        platform::{DataRoot, bootstrap_credential},
        store::StoreOwner,
        workspace::WorkspaceProjectConfig,
    };
    use std::{
        io,
        path::{Path, PathBuf},
        pin::Pin,
        process::Command,
        sync::{Arc, Mutex},
        task::{Context, Poll},
    };
    use tokio::{
        io::{AsyncRead, AsyncWrite, ReadBuf},
        sync::watch,
        task::JoinSet,
    };
    #[derive(Debug, PartialEq, Eq)]
    struct HookSetupIpcRequest {
        method: String,
        operation: Option<String>,
        source_id: Option<String>,
        client_request_id: Option<String>,
    }

    #[derive(Default)]
    struct HookSetupIpcTrace {
        requests: Vec<HookSetupIpcRequest>,
        preview: Option<serde_json::Value>,
        drop_operation_armed: bool,
        dropped_operation_replies: Vec<String>,
    }

    /// Wraps the real local IPC stream and drops one selected completed Store
    /// operation response. The server reaches this writer after `Store::call` returns.
    struct DropHookLocalReply {
        inner: Stream,
        request_frame: Vec<u8>,
        response_frame: Vec<u8>,
        current_method: Option<String>,
        current_operation: Option<String>,
        drop_operation: &'static str,
        drop_operation_response: bool,
        trace: Arc<Mutex<HookSetupIpcTrace>>,
    }

    impl DropHookLocalReply {
        fn new(
            inner: Stream,
            trace: Arc<Mutex<HookSetupIpcTrace>>,
            drop_operation: &'static str,
        ) -> Self {
            Self {
                inner,
                request_frame: Vec::new(),
                response_frame: Vec::new(),
                current_method: None,
                current_operation: None,
                drop_operation,
                drop_operation_response: false,
                trace,
            }
        }

        fn observe_request_bytes(&mut self, bytes: &[u8]) {
            for byte in bytes {
                if *byte != b'\n' {
                    self.request_frame.push(*byte);
                    continue;
                }

                let frame = std::mem::take(&mut self.request_frame);
                let Ok(request) = serde_json::from_slice::<serde_json::Value>(&frame) else {
                    continue;
                };
                let Some(method) = request.get("method").and_then(|value| value.as_str()) else {
                    continue;
                };
                if method == "client.hello" {
                    continue;
                }

                let params = request.get("params").cloned().unwrap_or_default();
                let local_operation = params
                    .get("operation")
                    .and_then(|value| value.get("operation"))
                    .and_then(|value| value.as_str())
                    .map(str::to_owned);
                let operation_value = params.get("operation").unwrap_or(&params);
                let source_id = operation_value
                    .get("source_id")
                    .or_else(|| params.get("source_id"))
                    .and_then(|value| value.as_str())
                    .map(str::to_owned);
                let client_request_id = operation_value
                    .get("client_request_id")
                    .or_else(|| params.get("client_request_id"))
                    .and_then(|value| value.as_str())
                    .map(str::to_owned);

                let is_target_operation = method == "hook.local.operation"
                    && local_operation.as_deref() == Some(self.drop_operation);
                self.current_method = Some(method.to_owned());
                self.current_operation = local_operation.clone();
                let mut trace = self.trace.lock().expect("test IPC trace mutex is healthy");
                self.drop_operation_response = is_target_operation && trace.drop_operation_armed;
                if self.drop_operation_response {
                    trace.drop_operation_armed = false;
                }
                trace.requests.push(HookSetupIpcRequest {
                    method: method.to_owned(),
                    operation: local_operation,
                    source_id,
                    client_request_id,
                });
            }
        }

        fn observe_response_bytes(&mut self, bytes: &[u8]) {
            for byte in bytes {
                if *byte != b'\n' {
                    self.response_frame.push(*byte);
                    continue;
                }

                let frame = std::mem::take(&mut self.response_frame);
                if self.current_method.as_deref() == Some("hook.local.operation")
                    && self.current_operation.as_deref() == Some("preview")
                    && let Ok(response) = serde_json::from_slice::<serde_json::Value>(&frame)
                    && let Some(preview) = response
                        .get("result")
                        .and_then(|result| result.get("preview"))
                {
                    self.trace
                        .lock()
                        .expect("test IPC trace mutex is healthy")
                        .preview = Some(preview.clone());
                }
            }
        }
    }

    impl AsyncRead for DropHookLocalReply {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buffer: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let this = self.get_mut();
            let before = buffer.filled().len();
            match Pin::new(&mut this.inner).poll_read(cx, buffer) {
                Poll::Ready(Ok(())) => {
                    let incoming = buffer.filled()[before..].to_vec();
                    this.observe_request_bytes(&incoming);
                    Poll::Ready(Ok(()))
                }
                other => other,
            }
        }
    }

    impl AsyncWrite for DropHookLocalReply {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            bytes: &[u8],
        ) -> Poll<io::Result<usize>> {
            let this = self.get_mut();
            if this.drop_operation_response {
                this.drop_operation_response = false;
                this.trace
                    .lock()
                    .expect("test IPC trace mutex is healthy")
                    .dropped_operation_replies
                    .push(this.drop_operation.to_owned());
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "qualification seam dropped the selected hook local-operation reply",
                )));
            }

            match Pin::new(&mut this.inner).poll_write(cx, bytes) {
                Poll::Ready(Ok(written)) => {
                    this.observe_response_bytes(&bytes[..written]);
                    Poll::Ready(Ok(written))
                }
                other => other,
            }
        }

        fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.get_mut().inner).poll_flush(cx)
        }

        fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
        }
    }

    fn fixture_git_executable() -> PathBuf {
        let executable_names = if cfg!(windows) {
            ["git.exe", "git"]
        } else {
            ["git", "git.exe"]
        };
        std::env::split_paths(&std::env::var_os("PATH").expect("test PATH is configured"))
            .flat_map(|directory| executable_names.map(|name| directory.join(name)))
            .find(|candidate| candidate.is_file())
            .and_then(|candidate| std::fs::canonicalize(candidate).ok())
            .expect("Git executable is available to the hook integration fixture")
    }

    fn run_fixture_git(git: &Path, repository: &Path, hooks: &Path, args: &[&str]) -> String {
        let output = Command::new(git)
            .arg("-c")
            .arg(format!("core.hooksPath={}", hooks.display()))
            .arg("-C")
            .arg(repository)
            .args(args)
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

    fn bootstrap_hook_workspace_in_child(config: &Config, project_id: &str) {
        let project = config
            .forge
            .projects
            .get(project_id)
            .expect("fixture project is configured");
        let workspace = config
            .workspace
            .projects
            .get(project_id)
            .expect("fixture workspace is configured");
        let workspace_root = workspace
            .allowed_roots
            .first()
            .expect("fixture has a workspace root");
        let output = Command::new(
            std::env::current_exe().expect("locate the bin test harness for host bootstrap"),
        )
        .arg("--exact")
        .arg("hooks::setup::tests::bootstrap_hook_workspace_through_public_host")
        .arg("--ignored")
        .arg("--nocapture")
        .stdin(std::process::Stdio::null())
        .env(
            "ELIOT_SWARM_TEST_HOOK_BOOTSTRAP_DATA_DIR",
            config.storage.data_dir.as_os_str(),
        )
        .env("ELIOT_SWARM_TEST_HOOK_BOOTSTRAP_PROJECT_ID", project_id)
        .env(
            "ELIOT_SWARM_TEST_HOOK_BOOTSTRAP_REPOSITORY",
            project.repository_path.as_os_str(),
        )
        .env(
            "ELIOT_SWARM_TEST_HOOK_BOOTSTRAP_CANONICAL_REPOSITORY",
            &project.canonical_repository,
        )
        .env(
            "ELIOT_SWARM_TEST_HOOK_BOOTSTRAP_REMOTE_NAME",
            &project.remote_name,
        )
        .env(
            "ELIOT_SWARM_TEST_HOOK_BOOTSTRAP_POLICY_REVISION",
            &project.policy_revision,
        )
        .env(
            "ELIOT_SWARM_TEST_HOOK_BOOTSTRAP_GIT",
            config.forge.git_executable.as_os_str(),
        )
        .env(
            "ELIOT_SWARM_TEST_HOOK_BOOTSTRAP_WORKSPACE_ROOT",
            workspace_root.as_os_str(),
        )
        .env(
            "ELIOT_SWARM_TEST_HOOK_BOOTSTRAP_TARGET_REFS",
            project.target_refs.join("\n"),
        )
        .output()
        .expect("run the public host startup fixture with stdin at EOF");
        assert!(
            output.status.success(),
            "public host startup fixture failed ({}): stdout={} stderr={}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }

    #[tokio::test]
    #[ignore = "child-process bootstrap for the connected hook setup fixture"]
    async fn bootstrap_hook_workspace_through_public_host() {
        let env_path = |name: &str| {
            PathBuf::from(
                std::env::var_os(name)
                    .unwrap_or_else(|| panic!("test host bootstrap requires {name}")),
            )
        };
        let project_id = std::env::var("ELIOT_SWARM_TEST_HOOK_BOOTSTRAP_PROJECT_ID")
            .expect("test host bootstrap requires a project id");
        let mut config = Config::default();
        config.storage.data_dir = env_path("ELIOT_SWARM_TEST_HOOK_BOOTSTRAP_DATA_DIR");
        config.forge.enabled = true;
        config.forge.git_executable = env_path("ELIOT_SWARM_TEST_HOOK_BOOTSTRAP_GIT");
        config.forge.projects.insert(
            project_id.clone(),
            ForgeProject {
                canonical_repository: std::env::var(
                    "ELIOT_SWARM_TEST_HOOK_BOOTSTRAP_CANONICAL_REPOSITORY",
                )
                .expect("test host bootstrap requires canonical repository"),
                repository_path: env_path("ELIOT_SWARM_TEST_HOOK_BOOTSTRAP_REPOSITORY"),
                remote_name: std::env::var("ELIOT_SWARM_TEST_HOOK_BOOTSTRAP_REMOTE_NAME")
                    .expect("test host bootstrap requires remote name"),
                policy_revision: std::env::var("ELIOT_SWARM_TEST_HOOK_BOOTSTRAP_POLICY_REVISION")
                    .expect("test host bootstrap requires policy revision"),
                target_refs: std::env::var("ELIOT_SWARM_TEST_HOOK_BOOTSTRAP_TARGET_REFS")
                    .expect("test host bootstrap requires target refs")
                    .lines()
                    .map(str::to_owned)
                    .collect(),
            },
        );
        config.workspace.projects.insert(
            project_id,
            WorkspaceProjectConfig {
                allowed_roots: vec![env_path("ELIOT_SWARM_TEST_HOOK_BOOTSTRAP_WORKSPACE_ROOT")],
            },
        );
        host::run_on_stdin_eof(config)
            .await
            .expect("public host startup initializes the fixture workspace before EOF shutdown");
    }

    #[test]
    fn hook_setup_cleanup_pending_result_bypasses_automatic_rollback() {
        let wire_result: crate::hooks::git::HookLocalResult =
            serde_json::from_value(serde_json::json!({
                "status":"cleanup_pending",
                "operation":"apply",
                "cause_code":"HOOK_GIT_CLEANUP_PENDING"
            }))
            .expect("parse the Store's CleanupPending wire result");
        let pending = match hook_local_response(wire_result) {
            Err(error) => error,
            Ok(_) => {
                panic!("a pending Store cleanup must not look like a completed local operation")
            }
        };

        assert_eq!(pending.code, "HOOK_GIT_CLEANUP_PENDING");
        assert!(
            setup_effect_requires_readback(&pending),
            "hook_setup must return before its automatic source rollback branch"
        );
        assert!(
            !setup_effect_requires_readback(&Error::new(
                "HOOK_GIT_FAILED",
                "ordinary apply failure",
            )),
            "ordinary apply failures continue to use the existing rollback path"
        );
    }

    #[test]
    fn hook_setup_lost_apply_or_readback_reply_preserves_source_identity() {
        for (code, message) in [
            (
                "OUTCOME_UNKNOWN",
                "reply missing; inspect the operation before retrying",
            ),
            ("OUTCOME_UNKNOWN", "server closed before reply"),
            (
                "OUTCOME_UNKNOWN",
                "reply could not be matched to the sent request",
            ),
            (
                "HOOK_LOCAL_RESPONSE_INVALID",
                "Store returned an invalid local hook operation result",
            ),
            (
                "QUALIFICATION_REPLY_DROPPED",
                "qualified reply withheld at the transport seam",
            ),
        ] {
            let lost_reply = Error::new(code, message);
            assert!(
                setup_effect_requires_readback(&lost_reply),
                "both Apply and Readback must exit before source revocation after {message}"
            );
            let preserved =
                unresolved_hook_setup(lost_reply, "retained-source", "retained-request");
            assert_eq!(preserved.code, code);
            assert!(
                preserved
                    .message
                    .contains("retained-source/retained-request")
            );
        }
        assert!(!setup_effect_requires_readback(&Error::new(
            "HOOK_GIT_FAILED",
            "confirmed local failure",
        )));
    }

    #[tokio::test]
    async fn hook_setup_lost_apply_or_readback_reply_is_reconciled_through_real_store_ipc() {
        for drop_operation in ["apply", "readback"] {
            run_hook_setup_lost_reply_case(drop_operation).await;
        }
    }

    async fn run_hook_setup_lost_reply_case(drop_operation: &'static str) {
        const PROJECT_ID: &str = "fixture";
        let request_id = format!("lost-hook-{drop_operation}-reply");

        let fixture_root =
            std::env::temp_dir().join(format!("swarm-hook-setup-lost-reply-{}", model::new_id()));
        let repository = fixture_root.join("repository");
        let workspace_root = fixture_root.join("workspaces");
        let empty_hooks = fixture_root.join("empty-hooks");
        std::fs::create_dir_all(&repository).expect("create fixture repository");
        std::fs::create_dir_all(&workspace_root).expect("create fixture workspace root");
        std::fs::create_dir_all(&empty_hooks).expect("create empty Git hooks directory");

        let git = fixture_git_executable();
        run_fixture_git(&git, &repository, &empty_hooks, &["init", "--quiet"]);
        run_fixture_git(
            &git,
            &repository,
            &empty_hooks,
            &["config", "user.name", "Hook Fixture"],
        );
        run_fixture_git(
            &git,
            &repository,
            &empty_hooks,
            &["config", "user.email", "hook-fixture@example.invalid"],
        );
        std::fs::write(repository.join("candidate.txt"), "hook setup fixture\n")
            .expect("write fixture commit");
        run_fixture_git(&git, &repository, &empty_hooks, &["add", "candidate.txt"]);
        run_fixture_git(
            &git,
            &repository,
            &empty_hooks,
            &["commit", "--quiet", "-m", "hook setup fixture"],
        );

        let configured_repository =
            std::fs::canonicalize(&repository).expect("canonicalize fixture repository");
        let git_dir = repository.join(".git");
        let hooks_dir = git_dir.join("hooks");
        std::fs::create_dir_all(&hooks_dir).expect("create repository hooks directory");
        let prior_executable = hooks_dir.join("post-commit.exe");
        let prior_executable_bytes = vec![0x5a; 1024 * 1024 + 64 * 1024 + 7];
        std::fs::write(&prior_executable, &prior_executable_bytes)
            .expect("seed large prior post-commit executable");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&prior_executable, std::fs::Permissions::from_mode(0o700))
                .expect("mark prior hook executable");
        }
        let expected_prior_digest = model::digest(&prior_executable_bytes);

        let mut config = Config::default();
        config.storage.data_dir = fixture_root.join("state");
        config.forge.enabled = true;
        config.forge.git_executable = git;
        config.forge.projects.insert(
            PROJECT_ID.to_owned(),
            ForgeProject {
                canonical_repository: "github.com/owner/hook-fixture".to_owned(),
                repository_path: configured_repository,
                remote_name: "origin".to_owned(),
                policy_revision: crate::policy::OWNER_POLICY_V2_ID.to_owned(),
                target_refs: vec!["refs/heads/main".to_owned()],
            },
        );
        config.workspace.projects.insert(
            PROJECT_ID.to_owned(),
            WorkspaceProjectConfig {
                allowed_roots: vec![workspace_root],
            },
        );
        let config = Arc::new(config);
        bootstrap_hook_workspace_in_child(&config, PROJECT_ID);
        let root = DataRoot::acquire(&config.storage.data_dir)
            .expect("reopen initialized test Store root");
        let operator_credential =
            bootstrap_credential(&root.path).expect("read initialized local operator credential");
        let owner = StoreOwner::start(root, config.clone(), operator_credential.clone())
            .await
            .expect("start real Store");

        let trace = Arc::new(Mutex::new(HookSetupIpcTrace {
            drop_operation_armed: true,
            ..HookSetupIpcTrace::default()
        }));
        let (stop, stopping) = watch::channel(false);
        let mut listener = ipc::Listener::bind(&config.storage.data_dir)
            .expect("bind the real local IPC endpoint");
        let store = owner.store.clone();
        let ipc_config = Arc::new(config.ipc.clone());
        let server_trace = trace.clone();
        let accept_task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            let mut stopping = stopping;
            loop {
                tokio::select! {
                    _ = stopping.changed() => break,
                    Some(_) = connections.join_next(), if !connections.is_empty() => {},
                    accepted = listener.accept() => match accepted {
                        Ok(stream) => {
                            let store = store.clone();
                            let ipc_config = ipc_config.clone();
                            let stopping = stopping.clone();
                            let trace = server_trace.clone();
                            connections.spawn(async move {
                                let stream: Stream = Box::new(DropHookLocalReply::new(
                                    stream,
                                    trace,
                                    drop_operation,
                                ));
                                let _ = ipc::serve(stream, store, ipc_config, stopping).await;
                            });
                        }
                        Err(_) => break,
                    }
                }
            }
            while connections.join_next().await.is_some() {}
        });

        let prepared = prepare_source_credential(&git_dir, PROJECT_ID, Some(&request_id))
            .expect("prepare a retained source identity before the caller sends it");
        let pending_descriptor = git_dir.join("eliot-hook-sources").join(format!(
            "setup-pending-{}.json",
            model::digest(PROJECT_ID.as_bytes())
        ));
        let source_setup_descriptor = git_dir
            .join("eliot-hook-sources")
            .join(&prepared.source_id)
            .join("setup-request.json");
        let pending_descriptor_before =
            std::fs::read(&pending_descriptor).expect("read pending source identity");
        let source_setup_descriptor_before =
            std::fs::read(&source_setup_descriptor).expect("read source setup identity");
        let credential_before =
            std::fs::read(&prepared.credential_path).expect("read retained source credential");

        let request_id_argument = Some(request_id.clone());
        let error = super::hook_setup(
            &config,
            &operator_credential,
            &request_id_argument,
            PROJECT_ID,
        )
        .await
        .unwrap_err();
        assert_eq!(error.code, "OUTCOME_UNKNOWN");
        assert!(error.message.contains(&prepared.source_id));
        assert!(error.message.contains(&request_id));

        let source_readback = ipc::call(
            &config.storage.data_dir,
            &prepared.credential,
            "hook.source.get",
            serde_json::json!({"source_id":prepared.source_id,"after":0,"limit":64}),
            &config.ipc,
        )
        .await
        .expect("read back the exact registered source identity");
        assert_eq!(
            source_readback["source"]["source_id"].as_str(),
            Some(prepared.source_id.as_str())
        );
        assert_eq!(
            source_readback["source"]["revoked_at_ms"],
            serde_json::Value::Null
        );

        let local_readback = super::hook_local_operation(
            &config,
            &operator_credential,
            PROJECT_ID,
            HookLocalOperation::Readback {
                source_id: prepared.source_id.clone(),
            },
        )
        .await
        .expect("explicitly reconcile the retained local installation");
        let HookLocalResult::Readback { readback } = local_readback else {
            panic!("explicit reconciliation returns local hook readback");
        };
        assert_eq!(readback.source_id, prepared.source_id);
        assert_eq!(readback.state, "installed");
        assert!(readback.wrapper_matches);
        assert!(readback.credential_file_present);

        assert_eq!(
            std::fs::read(&pending_descriptor).expect("pending identity remains retained"),
            pending_descriptor_before,
            "the caller preserves the original pending descriptor bytes"
        );
        assert_eq!(
            std::fs::read(&source_setup_descriptor)
                .expect("source setup identity remains retained"),
            source_setup_descriptor_before,
            "the source descriptor remains byte-identical through readback"
        );
        assert_eq!(
            std::fs::read(&prepared.credential_path).expect("source credential remains retained"),
            credential_before,
            "the pending source credential is never replaced after ambiguity"
        );

        let completion = super::hook_local_operation(
            &config,
            &operator_credential,
            PROJECT_ID,
            HookLocalOperation::CompleteSetup {
                source_id: prepared.source_id.clone(),
                client_request_id: prepared.client_request_id.clone(),
            },
        )
        .await
        .expect("finalize the same identity only after explicit readback");
        let HookLocalResult::Completed { source_id } = completion else {
            panic!("explicit reconciliation completes the retained setup identity");
        };
        assert_eq!(source_id, prepared.source_id);
        assert!(!pending_descriptor.exists());
        assert!(!source_setup_descriptor.exists());
        assert_eq!(
            std::fs::read(&prepared.credential_path)
                .expect("completed source keeps its installed credential"),
            credential_before
        );

        let trace = trace.lock().expect("test IPC trace mutex is healthy");
        assert_eq!(
            trace.dropped_operation_replies,
            vec![drop_operation.to_owned()]
        );
        assert!(!trace.drop_operation_armed);
        let preview = trace
            .preview
            .as_ref()
            .expect("the real hook_setup Preview response crossed local IPC");
        assert_eq!(
            preview["existing_post_commit_exe"]["sha256"].as_str(),
            Some(expected_prior_digest.as_str()),
            "the large existing executable digest reaches the actual setup preview"
        );

        let applications: Vec<_> = trace
            .requests
            .iter()
            .filter(|request| {
                request.method == "hook.local.operation"
                    && request.operation.as_deref() == Some("apply")
            })
            .collect();
        assert_eq!(
            applications.len(),
            1,
            "the native Apply effect is never replayed"
        );
        assert_eq!(
            applications[0].source_id.as_deref(),
            Some(prepared.source_id.as_str())
        );

        let source_setups: Vec<_> = trace
            .requests
            .iter()
            .filter(|request| request.method == "hook.source.setup")
            .collect();
        assert_eq!(source_setups.len(), 1);
        assert_eq!(
            source_setups[0].source_id.as_deref(),
            Some(prepared.source_id.as_str())
        );
        assert_eq!(
            source_setups[0].client_request_id.as_deref(),
            Some(request_id.as_str())
        );

        let local_operations: Vec<_> = trace
            .requests
            .iter()
            .filter(|request| request.method == "hook.local.operation")
            .filter_map(|request| request.operation.as_deref())
            .collect();
        let expected_local_operations = match drop_operation {
            "apply" => vec!["preview", "apply", "readback", "complete_setup"],
            "readback" => vec!["preview", "apply", "readback", "readback", "complete_setup"],
            _ => unreachable!("the test enumerates only Apply and Readback faults"),
        };
        assert_eq!(local_operations, expected_local_operations);
        let readbacks: Vec<_> = trace
            .requests
            .iter()
            .filter(|request| {
                request.method == "hook.local.operation"
                    && request.operation.as_deref() == Some("readback")
            })
            .collect();
        assert_eq!(
            readbacks.len(),
            if drop_operation == "apply" { 1 } else { 2 }
        );
        assert!(
            readbacks.iter().all(|request| {
                request.source_id.as_deref() == Some(prepared.source_id.as_str())
            })
        );
        let completions: Vec<_> = trace
            .requests
            .iter()
            .filter(|request| {
                request.method == "hook.local.operation"
                    && request.operation.as_deref() == Some("complete_setup")
            })
            .collect();
        assert_eq!(completions.len(), 1);
        assert_eq!(
            completions[0].source_id.as_deref(),
            Some(prepared.source_id.as_str())
        );
        assert_eq!(
            completions[0].client_request_id.as_deref(),
            Some(request_id.as_str())
        );
        assert!(!trace.requests.iter().any(|request| {
            request.method == "hook.source.revoke"
                || request.operation.as_deref() == Some("revoke")
                || request.operation.as_deref() == Some("rollback_setup")
        }));

        stop.send(true).expect("stop the local IPC fixture");
        accept_task
            .await
            .expect("join the local IPC fixture server");
    }
}
