//! Owned reader transport shared by no-Agent discovery and bound observation.
use super::{
    AgentKey, EventSource, PluginClient, PluginConnection, PluginHost, RPC_GRACE, STOP_GRACE,
    deadline, serve_plugin_requests, source_secret_snapshot,
};
use crate::{
    monitor::Monitoring,
    platform::{CustodyObserver, OwnedChild},
};
use eliot_core::{Agent, Error, Result, SecretPolicy, StderrSnapshot};
use eliot_plugin::{PluginManifest, StderrCapture, spawn_stderr};
use serde_json::{Value, json};
#[cfg(unix)]
use std::time::Duration;
use std::{path::PathBuf, sync::Arc};
use tokio::{
    task::JoinHandle,
    time::{Instant, timeout_at},
};

pub(super) struct ReaderTransport {
    pub child: OwnedChild,
    pub client: PluginClient,
    pub secrets: Arc<[String]>,
    pub credential: String,
    instance_dir: PathBuf,
    observer: Option<CustodyObserver>,
    requests: JoinHandle<()>,
    stderr: StderrCapture,
}

pub(super) struct ReaderClose {
    pub closed: bool,
    pub stderr: StderrSnapshot,
}

impl PluginHost {
    pub(super) async fn open_reader(
        &self,
        key: &AgentKey,
        harness: &str,
        profile: &Value,
        environment: &Value,
        observation: Option<&Agent>,
    ) -> Result<ReaderTransport> {
        let (instance_dir, credential) = self.create_instance_dir(key)?;
        let secrets = source_secret_snapshot(
            profile,
            environment,
            vec![self.state.owner_token.clone(), credential.clone()],
        );
        let mut child = match self.spawn_plugin_process(harness, &instance_dir) {
            Ok(child) => child,
            Err(error) => {
                let _ = std::fs::remove_dir(&instance_dir);
                return Err(error);
            }
        };
        let observer = child.observer().ok();
        let (Some(stdin), Some(stdout), Some(stderr)) =
            (child.take_stdin(), child.take_stdout(), child.take_stderr())
        else {
            // OwnedChild retains its original Job/group authority on this path.
            let _ = child.terminate();
            let _ = timeout_at(Instant::now() + STOP_GRACE, child.wait()).await;
            #[cfg(windows)]
            let _ = child.wait_family_empty(STOP_GRACE).await;
            drop(child);
            let _ = std::fs::remove_dir(&instance_dir);
            return Err(Error::new(
                "process_pipes",
                "reader plugin pipes are unavailable",
            ));
        };
        let stderr = spawn_stderr(stderr, Arc::new(SecretPolicy::new(secrets.iter())));
        let mut connection = PluginConnection::new(stdout, stdin);
        let client = connection.client();
        let requests = if let Some(agent) = observation {
            tokio::spawn(serve_plugin_requests(
                connection,
                EventSource {
                    store: Arc::clone(&self.state.store),
                    bus: self.state.bus.clone(),
                    monitoring: self.state.monitoring.clone(),
                    source_credentials: Arc::clone(&secrets),
                    state: Arc::downgrade(&self.state),
                    key: key.clone(),
                    agent_id: agent.id.clone(),
                    generation: agent.generation,
                    expected_credential: credential.clone(),
                    observation: Some(agent.clone()),
                },
            ))
        } else {
            let replies = client.clone();
            tokio::spawn(async move {
                while let Some(request) = connection.next_request().await {
                    let _ = replies
                        .respond(
                            request.id,
                            Err(Error::new(
                                "unsupported",
                                "discovery cannot publish Agent facts",
                            )),
                            deadline(),
                        )
                        .await;
                }
            })
        };
        self.state
            .secrets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(credential.clone());
        Ok(ReaderTransport {
            child,
            client,
            secrets,
            credential,
            instance_dir,
            observer,
            requests,
            stderr,
        })
    }
}

impl ReaderTransport {
    pub(super) fn diagnose(&self, error: Error) -> Error {
        super::diagnose_error(error, &self.stderr.view(), &self.secrets)
    }
    pub(super) async fn initialize(
        &mut self,
        harness: &str,
        until: Instant,
    ) -> Result<PluginManifest> {
        let value = self.client.call("initialize", json!({
            "protocol": "eliot-plugin/1", "credential": self.credential,
            "instance_dir": self.instance_dir.to_string_lossy(), "config": {"harness": harness},
        }), until).await?;
        let manifest: PluginManifest = serde_json::from_value(value)
            .map_err(|_| Error::new("unsupported", "invalid reader manifest"))?;
        if manifest.protocol != "eliot-plugin/1" || manifest.name != harness || !manifest.available
        {
            return Err(Error::new(
                "unsupported",
                "plugin does not support the requested reader",
            ));
        }
        Ok(manifest)
    }

    pub(super) async fn observe_os(
        &mut self,
        monitoring: &Monitoring,
        agent: &Agent,
    ) -> Result<()> {
        let observer = self.observer.clone().ok_or_else(|| {
            Error::new(
                "process_observation",
                "reader custody observation is unavailable",
            )
        })?;
        monitoring.attach(agent, observer).await
    }

    pub(super) async fn close(mut self, host: &PluginHost, agent: Option<&Agent>) -> ReaderClose {
        let shutdown = Instant::now() + RPC_GRACE;
        let _ = self.client.call("shutdown", json!({}), shutdown).await;
        let _ = self.client.close_input(shutdown).await;
        #[cfg(unix)]
        let _ = self.child.terminate();
        let mut reaped = matches!(
            timeout_at(Instant::now() + RPC_GRACE, self.child.wait()).await,
            Ok(Ok(_))
        );
        if !reaped {
            let _ = self.child.terminate();
            reaped = matches!(
                timeout_at(Instant::now() + STOP_GRACE, self.child.wait()).await,
                Ok(Ok(_))
            );
        }
        #[cfg(windows)]
        let empty = {
            let mut empty = matches!(self.child.wait_family_empty(RPC_GRACE).await, Ok(true));
            if !empty {
                let _ = self.child.terminate();
                empty = matches!(self.child.wait_family_empty(STOP_GRACE).await, Ok(true));
            }
            empty
        };
        #[cfg(unix)]
        let empty = {
            let until = Instant::now() + STOP_GRACE;
            loop {
                match self
                    .observer
                    .as_ref()
                    .map(CustodyObserver::known_process_group_empty)
                {
                    Some(Ok(true)) => break true,
                    Some(Ok(false)) if Instant::now() < until => {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    _ => break false,
                }
            }
        };
        #[cfg(not(any(unix, windows)))]
        let empty = false;
        if let Some(agent) = agent {
            let _ = host.state.monitoring.sample_generation(agent).await;
        }
        let drained = matches!(
            timeout_at(Instant::now() + RPC_GRACE, &mut self.requests).await,
            Ok(Ok(()))
        );
        self.requests.abort();
        if !drained {
            let _ = self.requests.await;
        }
        drop(self.child);
        let stderr = self.stderr.finish(Instant::now() + RPC_GRACE).await;
        host.remove_plugin_secret(&self.credential);
        let _ = std::fs::remove_dir(&self.instance_dir);
        ReaderClose {
            closed: reaped && empty && drained,
            stderr,
        }
    }
}
