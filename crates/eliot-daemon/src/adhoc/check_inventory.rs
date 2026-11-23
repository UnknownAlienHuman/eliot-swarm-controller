use super::*;
use eliot_core::api::{HarnessCandidate, harness::SUPPORTED_HARNESSES};
use std::{collections::HashMap, sync::Arc};

#[derive(Default)]
pub(crate) struct CheckInventory {
    next_refresh: Option<Instant>,
    observations: HashMap<&'static str, Observation>,
}

#[derive(Clone)]
struct Observation {
    fingerprints: Vec<VersionKey>,
    selected: Option<(HarnessCandidate, VersionKey)>,
}

impl CheckInventory {
    /// Reuse observations across supervisor restarts in the same daemon.
    pub(crate) async fn refresh(policy: &Arc<core::SecretPolicy>) -> Self {
        static INVENTORY: std::sync::LazyLock<tokio::sync::Mutex<CheckInventory>> =
            std::sync::LazyLock::new(|| tokio::sync::Mutex::new(CheckInventory::default()));
        let mut state = INVENTORY.lock().await;
        if state.next_refresh.is_some_and(|next| Instant::now() < next) {
            crate::logging::record(policy, "harness.check_maintenance_tick", &[], None);
        } else {
            state.next_refresh = Some(
                Instant::now() + std::time::Duration::from_secs(core::HARNESS_CHECK_INVENTORY_SECS),
            );
            for &harness in SUPPORTED_HARNESSES {
                let previous = state.observations.get(harness).cloned();
                let probe_policy = Arc::clone(policy);
                let deadline = Instant::now() + DISCOVERY_BUDGET;
                match blocking_discovery(deadline, move || {
                    observe_installation(harness, previous, &probe_policy, deadline)
                })
                .await
                {
                    Ok(observation) => {
                        state.observations.insert(harness, observation);
                    }
                    Err(error) => {
                        state.observations.remove(harness);
                        crate::logging::record(
                            policy,
                            "harness.check_inventory_unavailable",
                            &[],
                            Some(&error),
                        );
                    }
                }
            }
        }
        Self {
            next_refresh: state.next_refresh,
            observations: state.observations.clone(),
        }
    }

    pub(crate) fn candidate(&self, harness: &str) -> Option<&HarnessCandidate> {
        let (candidate, fingerprint) = self.observations.get(harness)?.selected.as_ref()?;
        VersionKey::read(&candidate.cmd, None)
            .is_some_and(|current| current == *fingerprint)
            .then_some(candidate)
    }
}

fn observe_installation(
    harness: &str,
    previous: Option<Observation>,
    policy: &core::SecretPolicy,
    deadline: Instant,
) -> Observation {
    let fingerprints = installation_fingerprints(harness).unwrap_or_default();
    if let Some(previous) = previous
        && previous.fingerprints == fingerprints
        && previous.selected.is_some()
    {
        return previous;
    }
    let selected = if fingerprints.is_empty() {
        None
    } else {
        let passive = discovery_for(harness)
            .is_ok_and(|decl| matches!(decl.strategy, InstallStrategy::MetadataVersion(_)));
        if !passive {
            crate::logging::record(
                policy,
                "harness.check_inventory_probe",
                &[("method", "--version"), ("harness", harness)],
                None,
            );
        }
        let entry = harness_installation(harness, deadline);
        entry.get("cmd").and_then(Value::as_str).and_then(|cmd| {
            let key = VersionKey::read(cmd, None)?;
            if !fingerprints.contains(&key) {
                return None;
            }
            let version = entry.get("version")?.as_str()?.to_owned();
            Some((
                HarnessCandidate {
                    cmd: cmd.to_owned(),
                    version: Some(version),
                },
                key,
            ))
        })
    };
    Observation {
        fingerprints,
        selected,
    }
}

fn installation_fingerprints(harness: &str) -> core::Result<Vec<VersionKey>> {
    let decl = discovery_for(harness)?;
    let cmds = match decl.strategy {
        InstallStrategy::ByName => vec![harness.to_owned()],
        InstallStrategy::FirstExisting | InstallStrategy::MetadataVersion(_) => {
            discover_first_existing(&decl)?.into_iter().collect()
        }
        InstallStrategy::NewestVerified => (decl.candidates)()?
            .iter()
            .map(|path| path.to_string_lossy().into_owned())
            .collect(),
    };
    let mut fingerprints = cmds
        .iter()
        .filter_map(|cmd| resolve_native_executable(cmd, harness).ok())
        .filter_map(|cmd| VersionKey::read(&cmd, None))
        .collect::<Vec<_>>();
    fingerprints.sort_unstable_by(|left, right| left.program.cmp(&right.program));
    fingerprints.dedup();
    Ok(fingerprints)
}
