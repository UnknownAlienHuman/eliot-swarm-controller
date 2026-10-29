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
use eliot_plugin::{PluginManifest, StderrCapture, spawn_stderr, sync::LockExt};
use serde_json::{Value, json};
#[cfg(unix)]
use std::time::Duration;
use std::{path::PathBuf, sync::Arc};
use tokio::{
    task::JoinHandle,
    time::{Instant, timeout_at},
};

// Covers shutdown/input close (2s), leader waits (2s + 5s), Windows Job
// waits (2s + 5s), request drain (2s), and stderr drain (2s). Unix uses a
// single 5s process-group check, so its worst case is 18s.
pub(super) const READER_CLOSE_BUDGET: std::time::Duration = std::time::Duration::from_secs(20);

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

pub(super) struct ReaderOpen {
    pub result: Result<ReaderTransport>,
    pub cleanup_confirmed: bool,
}

impl ReaderOpen {
    fn failed_spawn(error: Error) -> Self {
        Self {
            result: Err(error),
            cleanup_confirmed: false,
        }
    }
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
        self.open_reader_with_cleanup(key, harness, profile, environment, observation)
            .await
            .result
    }

    pub(super) async fn open_reader_with_cleanup(
        &self,
        key: &AgentKey,
        harness: &str,
        profile: &Value,
        environment: &Value,
        observation: Option<&Agent>,
    ) -> ReaderOpen {
        let (instance_dir, credential) = match self.create_instance_dir(key) {
            Ok(instance) => instance,
            Err(error) => {
                return ReaderOpen {
                    result: Err(error),
                    cleanup_confirmed: true,
                };
            }
        };
        let secrets = source_secret_snapshot(
            profile,
            environment,
            vec![self.state.owner_token.clone(), credential.clone()],
        );
        let mut child = match self.spawn_plugin_process(harness, &instance_dir) {
            Ok(child) => child,
            Err(error) => {
                let _ = std::fs::remove_dir(&instance_dir);
                return ReaderOpen::failed_spawn(error);
            }
        };
        let observer = child.observer().ok();
        let (Some(stdin), Some(stdout), Some(stderr)) =
            (child.take_stdin(), child.take_stdout(), child.take_stderr())
        else {
            // OwnedChild retains its original Job/group authority on this path.
            let _ = child.terminate();
            let reaped = matches!(
                timeout_at(Instant::now() + STOP_GRACE, child.wait()).await,
                Ok(Ok(_))
            );
            #[cfg(windows)]
            let empty = matches!(child.wait_family_empty(STOP_GRACE).await, Ok(true));
            #[cfg(unix)]
            let empty = {
                let until = Instant::now() + STOP_GRACE;
                loop {
                    match observer
                        .as_ref()
                        .map(CustodyObserver::known_process_group_empty)
                    {
                        Some(Ok(true)) => break true,
                        Some(Ok(false)) if Instant::now() < until => {
                            tokio::time::sleep_until(
                                (Instant::now() + Duration::from_millis(10)).min(until),
                            )
                            .await;
                        }
                        _ => break false,
                    }
                }
            };
            #[cfg(not(any(unix, windows)))]
            let empty = false;
            drop(child);
            let _ = std::fs::remove_dir(&instance_dir);
            return ReaderOpen {
                result: Err(Error::new(
                    "process_pipes",
                    "reader plugin pipes are unavailable",
                )),
                cleanup_confirmed: reaped && empty,
            };
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
        self.state.secrets.lock_recover().insert(credential.clone());
        ReaderOpen {
            result: Ok(ReaderTransport {
                child,
                client,
                secrets,
                credential,
                instance_dir,
                observer,
                requests,
                stderr,
            }),
            cleanup_confirmed: false,
        }
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

    pub(super) async fn close(self, host: &PluginHost, agent: Option<&Agent>) -> ReaderClose {
        self.close_until(host, agent, Instant::now() + READER_CLOSE_BUDGET)
            .await
    }

    pub(super) async fn close_until(
        mut self,
        host: &PluginHost,
        agent: Option<&Agent>,
        close_until: Instant,
    ) -> ReaderClose {
        let shutdown = bounded_deadline(close_until, RPC_GRACE);
        let _ = self.client.call("shutdown", json!({}), shutdown).await;
        let _ = self.client.close_input(shutdown).await;
        #[cfg(unix)]
        let _ = self.child.terminate();
        let mut reaped = matches!(
            timeout_at(bounded_deadline(close_until, RPC_GRACE), self.child.wait()).await,
            Ok(Ok(_))
        );
        if !reaped {
            let _ = self.child.terminate();
            reaped = matches!(
                timeout_at(bounded_deadline(close_until, STOP_GRACE), self.child.wait()).await,
                Ok(Ok(_))
            );
        }
        #[cfg(windows)]
        let empty = {
            let mut empty = matches!(
                self.child
                    .wait_family_empty(remaining(bounded_deadline(close_until, RPC_GRACE)))
                    .await,
                Ok(true)
            );
            if !empty {
                let _ = self.child.terminate();
                empty = matches!(
                    self.child
                        .wait_family_empty(remaining(bounded_deadline(close_until, STOP_GRACE)))
                        .await,
                    Ok(true)
                );
            }
            empty
        };
        #[cfg(unix)]
        let empty = {
            let until = bounded_deadline(close_until, STOP_GRACE);
            loop {
                match self
                    .observer
                    .as_ref()
                    .map(CustodyObserver::known_process_group_empty)
                {
                    Some(Ok(true)) => break true,
                    Some(Ok(false)) if Instant::now() < until => {
                        tokio::time::sleep_until(
                            (Instant::now() + Duration::from_millis(10)).min(until),
                        )
                        .await;
                    }
                    _ => break false,
                }
            }
        };
        #[cfg(not(any(unix, windows)))]
        let empty = false;
        if let Some(agent) = agent {
            let _ = timeout_at(close_until, host.state.monitoring.sample_generation(agent)).await;
        }
        let drained = matches!(
            timeout_at(bounded_deadline(close_until, RPC_GRACE), &mut self.requests).await,
            Ok(Ok(()))
        );
        self.requests.abort();
        if !drained {
            let _ = self.requests.await;
        }
        drop(self.child);
        let stderr = self
            .stderr
            .finish(bounded_deadline(close_until, RPC_GRACE))
            .await;
        host.remove_plugin_secret(&self.credential);
        let _ = std::fs::remove_dir(&self.instance_dir);
        ReaderClose {
            closed: reaped && empty && drained,
            stderr,
        }
    }
}

fn bounded_deadline(close_until: Instant, budget: std::time::Duration) -> Instant {
    (Instant::now() + budget).min(close_until)
}

#[cfg(windows)]
fn remaining(until: Instant) -> std::time::Duration {
    let now = Instant::now();
    if until > now {
        until - now
    } else {
        std::time::Duration::ZERO
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spawn_error_does_not_confirm_reader_cleanup() {
        let opened = ReaderOpen::failed_spawn(Error::new("platform_io", "ambiguous spawn failure"));
        assert!(!opened.cleanup_confirmed);
        assert!(matches!(opened.result, Err(error) if error.code == "platform_io"));
    }
}
