use eliot_core::api::{
    UPDATE_HEALTH_MAX_SUBJECTS, UpdateHealthRequest, UpdateHealthResponse, UpdateStartEvidence,
    UpdateStartState, UpdateStartSubject,
};
use serde::Serialize;
use std::{path::Path, time::Duration};
use tokio::{
    task::JoinSet,
    time::{Instant, sleep_until, timeout_at},
};

const RPC: &str = "daemon.update.health";
const OBSERVE_FOR: Duration = Duration::from_secs(30);
const PROBE_FOR: Duration = Duration::from_secs(12);

#[derive(Default)]
pub(super) struct PreviousHealth {
    samples: Vec<UpdateStartSubject>,
    accepted: Vec<UpdateStartSubject>,
    observed: bool,
}

#[derive(Serialize)]
pub(super) struct HealthReport {
    state: &'static str,
    source: &'static str,
    coverage_complete: bool,
    requested: usize,
    evidence: Vec<UpdateStartEvidence>,
    previously_accepted: Vec<UpdateStartSubject>,
}

impl HealthReport {
    pub(super) fn refusal(&self) -> Option<&UpdateStartEvidence> {
        self.evidence.iter().find(|evidence| {
            evidence.deterministic_swarm_refusal()
                && self.previously_accepted.contains(&evidence.subject)
        })
    }

    pub(super) fn is_ready(&self) -> bool {
        self.state == "ready"
    }
}

async fn read(
    path: &Path,
    request: &UpdateHealthRequest,
    deadline: Instant,
) -> Option<UpdateHealthResponse> {
    timeout_at(
        deadline,
        eliot_daemon::client_call_typed(path, RPC, request),
    )
    .await
    .ok()?
    .ok()
}

async fn probes(
    path: &Path,
    subjects: impl IntoIterator<Item = UpdateStartSubject>,
    deadline: Instant,
) -> Vec<UpdateStartEvidence> {
    let mut rest = subjects
        .into_iter()
        .take(UPDATE_HEALTH_MAX_SUBJECTS + eliot_core::api::SUPPORTED_HARNESSES.len());
    let mut tasks = JoinSet::new();
    let launch = |tasks: &mut JoinSet<_>, subject: UpdateStartSubject| {
        let path = path.to_owned();
        tasks.spawn(async move {
            match read(
                &path,
                &UpdateHealthRequest::Probe {
                    subject: subject.clone(),
                },
                deadline,
            )
            .await
            {
                Some(UpdateHealthResponse::Probe { evidence }) if evidence.subject == subject => {
                    evidence
                }
                _ => UpdateStartEvidence {
                    subject,
                    state: UpdateStartState::Unknown,
                    code: None,
                    origin: None,
                    command_id: None,
                },
            }
        });
    };
    for subject in rest.by_ref().take(8) {
        launch(&mut tasks, subject);
    }
    let mut evidence = Vec::new();
    while let Some(result) = tasks.join_next().await {
        if let Ok(result) = result {
            evidence.push(result);
        }
        if let Some(subject) = rest.next() {
            launch(&mut tasks, subject);
        }
    }
    evidence.sort_by(|a, b| a.subject.agent.cmp(&b.subject.agent));
    evidence
}

pub(super) async fn capture_previous(path: &Path) -> PreviousHealth {
    let deadline = Instant::now() + PROBE_FOR;
    let Some(UpdateHealthResponse::Inventory {
        mut live, samples, ..
    }) = read(path, &UpdateHealthRequest::Inventory, deadline).await
    else {
        return PreviousHealth::default();
    };
    for sample in &samples {
        if !live.contains(sample) {
            live.push(sample.clone());
        }
    }
    PreviousHealth {
        samples,
        accepted: probes(path, live, deadline)
            .await
            .into_iter()
            .filter(|row| row.state == UpdateStartState::Ready)
            .map(|row| row.subject)
            .collect(),
        observed: true,
    }
}

pub(super) async fn assess_update(path: &Path, previous: PreviousHealth) -> HealthReport {
    let deadline = Instant::now() + OBSERVE_FOR;
    let (mut last_requested, mut last_attempts) = (0, Vec::new());
    let (source, complete, requested, evidence) = loop {
        let Some(UpdateHealthResponse::Boot {
            finished,
            complete,
            requested,
            attempts,
        }) = read(path, &UpdateHealthRequest::Boot, deadline).await
        else {
            break ("boot_continuations", false, last_requested, last_attempts);
        };
        if finished {
            if !complete || requested > 0 {
                break ("boot_continuations", complete, requested, attempts);
            }
            let probe_deadline = Instant::now() + PROBE_FOR;
            // Use the same subjects the previous daemon selected. When it
            // lacked the API, collect candidate evidence without a baseline.
            let samples = if previous.observed {
                previous.samples
            } else {
                let Some(UpdateHealthResponse::Inventory { samples, .. }) =
                    read(path, &UpdateHealthRequest::Inventory, probe_deadline).await
                else {
                    break ("readiness_probes", false, 0, Vec::new());
                };
                samples
            };
            let count = samples.len();
            let evidence = probes(path, samples, probe_deadline).await;
            break ("readiness_probes", evidence.len() == count, count, evidence);
        }
        last_requested = requested;
        last_attempts = attempts;
        sleep_until((Instant::now() + Duration::from_millis(200)).min(deadline)).await;
    };
    let mut result = HealthReport {
        state: "unknown",
        source,
        coverage_complete: complete,
        requested,
        evidence,
        previously_accepted: previous.accepted,
    };
    if result.refusal().is_some() {
        result.state = "deterministic_refusal";
    } else if complete
        && result
            .evidence
            .iter()
            .all(|row| row.state == UpdateStartState::Ready)
    {
        result.state = "ready";
    }
    result
}
