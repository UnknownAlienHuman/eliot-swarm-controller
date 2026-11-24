use super::{WRITE_LANE, existing, repository_path};
use crate::git_monitor::GitProbe;
use eliot_core::{Error, Result};
use eliot_store::{CheckpointRetentionGeneration, Store};
use std::{fmt::Write as _, path::Path, time::Duration};
use tokio::{sync::Mutex, time::Instant};

const SCAN_LIMIT: usize = 32;
const SWEEP_TIME: Duration = Duration::from_secs(15);
const COLLECTION_TIME: Duration = Duration::from_secs(15);

pub(crate) async fn maintain(
    store: &Mutex<Store>,
    data_root: &Path,
    cutoff: Option<i64>,
    after: &mut Option<(i64, i64)>,
) -> Result<()> {
    if matches!(std::fs::symlink_metadata(repository_path(data_root)?), Err(error) if error.kind() == std::io::ErrorKind::NotFound)
    {
        return Ok(());
    }
    let Ok(_lane) = WRITE_LANE.try_lock() else {
        return Ok(());
    };
    let generations = {
        let store = store.lock().await;
        // Capture and publication are separated by a durable Store intent.
        // Its objects may still be unreachable before publication.
        if store.has_git_checkpoint_intents()? {
            return Ok(());
        }
        match cutoff {
            Some(cutoff) => store.checkpoint_retention_generations(*after, SCAN_LIMIT, cutoff)?,
            None => Vec::new(),
        }
    };
    let root = existing(data_root).await?;
    let git = GitProbe::new()?;
    let deadline = Instant::now() + SWEEP_TIME;
    if let Some(cutoff) = cutoff {
        for generation in &generations {
            sweep_generation(&git, &root, generation, cutoff, deadline).await?;
            *after = Some((generation.agent_id, generation.generation));
        }
    }
    if generations.len() < SCAN_LIMIT {
        *after = None;
    }
    git.gc_private_repository(&root, Instant::now() + COLLECTION_TIME)
        .await
}

async fn sweep_generation(
    git: &GitProbe,
    root: &Path,
    generation: &CheckpointRetentionGeneration,
    cutoff: i64,
    deadline: Instant,
) -> Result<()> {
    let prefix = format!(
        "refs/eliot/checkpoints/a{}/g{}/",
        generation.agent_id, generation.generation
    );
    let output = git
        .workspace_command(
            root,
            &[
                "for-each-ref",
                "--count=128",
                "--format=%(refname) %(objectname)",
                &prefix,
            ],
            deadline,
        )
        .await?;
    if output.code != 0 {
        return Err(Error::new(
            "unavailable",
            "private checkpoint refs could not be listed",
        ));
    }
    let current = generation.checkpoint_ref.as_deref();
    let mut writes = String::new();
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let Some((reference, oid)) = line.split_once(' ') else {
            return Err(Error::new(
                "source_integrity",
                "invalid private Git ref inventory",
            ));
        };
        let old = reference
            .strip_prefix(&prefix)
            .and_then(|label| label.split_once('-'))
            .is_some_and(|(ts, id)| {
                ts.len() == 16
                    && ts.parse::<i64>().is_ok_and(|ts| ts <= cutoff)
                    && ulid::Ulid::from_string(id).is_ok()
            });
        // A task binding can change between captures. Keep the exact Store
        // pointer the restore API accepts for each retained generation.
        if old && eliot_core::api::is_git_oid(oid) && current != Some(reference) {
            writeln!(&mut writes, "delete {reference} {oid}")
                .map_err(|_| Error::new("internal", "checkpoint deletion could not be encoded"))?;
        }
    }
    if !writes.is_empty() {
        let result = git
            .workspace_input_command(
                root,
                &["update-ref", "--stdin"],
                writes.as_bytes(),
                deadline,
            )
            .await?;
        if result.code != 0 {
            return Err(Error::new(
                "unavailable",
                "expired checkpoint refs could not be pruned",
            ));
        }
    }
    Ok(())
}
