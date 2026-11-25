use crate::{
    git_monitor::GitProbe,
    work_truth::{CheckpointManifest, MAX_CHECKPOINT_FILES, SnapshotFile},
};
use eliot_core::{
    Error, Result,
    api::{
        CheckpointFileMetadata, CheckpointGetCursor, CheckpointGetRequest, CheckpointGetResponse,
        CheckpointListCursor, CheckpointListEntry, CheckpointListRequest, CheckpointListResponse,
        inference::sha256_hex, is_git_oid,
    },
};
use sha2::{Digest, Sha256};
use std::{path::Path, time::Duration};
use tokio::time::Instant;

const INVENTORY_LIMIT: usize = 2048;
const MANIFEST_LIMIT: usize = 256 * 1024;
const PAGE_BYTES: usize = 64 * 1024;
const READ_TIME: Duration = Duration::from_secs(30);

async fn retained_inventory(
    root: &Path,
    request: &CheckpointListRequest,
    pending: Option<&str>,
    deadline: Instant,
) -> Result<(Vec<CheckpointListEntry>, String)> {
    let prefix = format!(
        "refs/eliot/checkpoints/{}/g{}/",
        request.agent, request.generation
    );
    let git = GitProbe::new()?;
    let output = git
        .workspace_command(
            root,
            &[
                "for-each-ref",
                &format!("--count={}", INVENTORY_LIMIT + 1),
                "--sort=-refname",
                "--format=%(refname) %(objectname)",
                &prefix,
            ],
            deadline,
        )
        .await?;
    if output.code != 0 {
        return Err(Error::new("unavailable", "checkpoint inventory failed"));
    }
    let raw = String::from_utf8(output.stdout)
        .map_err(|_| Error::new("unavailable", "checkpoint inventory is unreadable"))?;
    let lines: Vec<_> = raw.lines().collect();
    if lines.len() > INVENTORY_LIMIT {
        return Err(Error::new(
            "unavailable",
            "checkpoint inventory exceeds the native output limit",
        ));
    }
    let mut refs = Vec::with_capacity(lines.len());
    for line in lines {
        let (reference, oid) = line.split_once(' ').ok_or_else(|| {
            Error::new("source_integrity", "checkpoint inventory row is malformed")
        })?;
        if !reference.starts_with(&prefix) || reference.len() > 512 || !is_git_oid(oid) {
            return Err(Error::new(
                "source_integrity",
                "checkpoint inventory identity is malformed",
            ));
        }
        if Some(reference) != pending {
            refs.push(CheckpointListEntry {
                checkpoint_ref: reference.to_owned(),
                oid: oid.to_owned(),
            });
        }
    }
    let mut inventory = Sha256::new();
    for entry in &refs {
        inventory.update(entry.checkpoint_ref.as_bytes());
        inventory.update(b" ");
        inventory.update(entry.oid.as_bytes());
        inventory.update(b"\n");
    }
    let revision = inventory
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    Ok((refs, revision))
}

pub(crate) async fn list(
    root: Option<&Path>,
    request: &CheckpointListRequest,
    pending: Option<&str>,
) -> Result<CheckpointListResponse> {
    let (refs, revision) = if let Some(root) = root {
        let deadline = Instant::now() + READ_TIME;
        let _lane = crate::work_truth_repo::read_lane(deadline).await?;
        retained_inventory(root, request, pending, deadline).await?
    } else {
        (Vec::new(), sha256_hex(&[]))
    };
    let start = if let Some(cursor) = &request.cursor {
        if cursor.revision != revision {
            return Err(Error::new("stale_cursor", "checkpoint inventory changed"));
        }
        let index = refs
            .iter()
            .position(|e| e.checkpoint_ref == cursor.after)
            .ok_or_else(|| {
                Error::new(
                    "invalid_cursor",
                    "checkpoint cursor position does not exist",
                )
            })?;
        index + 1
    } else {
        0
    };
    let has_more = refs.len() - start > request.limit;
    let entries: Vec<_> = refs.into_iter().skip(start).take(request.limit).collect();
    let next = has_more.then(|| CheckpointListCursor {
        agent: request.agent.clone(),
        generation: request.generation,
        revision: revision.clone(),
        after: entries
            .last()
            .expect("nonempty page")
            .checkpoint_ref
            .clone(),
    });
    Ok(CheckpointListResponse {
        agent: request.agent.clone(),
        generation: request.generation,
        revision,
        entries,
        next,
    })
}

pub(crate) async fn get(
    root: &Path,
    request: &CheckpointGetRequest,
) -> Result<CheckpointGetResponse> {
    let deadline = Instant::now() + READ_TIME;
    let _lane = crate::work_truth_repo::read_lane(deadline).await?;
    let git = GitProbe::new()?;
    let actual = crate::work_truth::checkpoint_reference(root, &request.checkpoint_ref).await?;
    let actual =
        actual.ok_or_else(|| Error::new("not_found", "checkpoint ref is no longer retained"))?;
    if actual != request.oid {
        return Err(Error::new(
            "stale_revision",
            "checkpoint ref no longer names the listed object",
        ));
    }
    let tree = git
        .workspace_command(root, &["ls-tree", "-z", &request.oid], deadline)
        .await?;
    if tree.code != 0 {
        return Err(Error::new(
            "not_found",
            "checkpoint manifest is unavailable",
        ));
    }
    let row = tree
        .stdout
        .split(|b| *b == 0)
        .find(|row| row.ends_with(b"\tmanifest.json"))
        .ok_or_else(|| Error::new("unavailable", "checkpoint manifest entry is missing"))?;
    let text = std::str::from_utf8(row)
        .map_err(|_| Error::new("unavailable", "checkpoint manifest entry is unreadable"))?;
    let blob = text
        .split_whitespace()
        .nth(2)
        .filter(|oid| is_git_oid(oid))
        .ok_or_else(|| Error::new("unavailable", "checkpoint manifest object is invalid"))?;
    let size = git
        .workspace_command(root, &["cat-file", "-s", blob], deadline)
        .await?;
    let bytes: usize = std::str::from_utf8(&size.stdout)
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .filter(|n| size.code == 0 && *n <= MANIFEST_LIMIT)
        .ok_or_else(|| {
            Error::new(
                "unavailable",
                "checkpoint manifest exceeds its metadata limit",
            )
        })?;
    let bytes = git.workspace_blob(root, blob, bytes, deadline).await?;
    let manifest: CheckpointManifest = serde_json::from_slice(&bytes)
        .map_err(|_| Error::new("unavailable", "checkpoint manifest is unreadable"))?;
    if manifest.version != 1
        || manifest.agent != request.agent
        || manifest.generation != request.generation
        || !is_git_oid(&manifest.head)
        || manifest.staged.len().max(manifest.worktree.len()) > MAX_CHECKPOINT_FILES
    {
        return Err(Error::new(
            "source_integrity",
            "checkpoint manifest binding is invalid",
        ));
    }
    let convert = |files: Vec<SnapshotFile>| -> Result<Vec<CheckpointFileMetadata>> {
        files
            .into_iter()
            .map(|f| {
                if !super::valid_relative_git_path(&f.path)
                    || f.oid.as_deref().is_some_and(|oid| !is_git_oid(oid))
                {
                    return Err(Error::new(
                        "source_integrity",
                        "checkpoint metadata path or object is invalid",
                    ));
                }
                Ok(CheckpointFileMetadata {
                    path: f.path,
                    mode: f.mode,
                    oid: f.oid,
                })
            })
            .collect()
    };
    let staged = convert(manifest.staged)?;
    let worktree = convert(manifest.worktree)?;
    let total = staged.len() + worktree.len();
    let after = request.cursor.as_ref().map_or(0, |cursor| cursor.after);
    if after > total {
        return Err(Error::new(
            "invalid_cursor",
            "checkpoint metadata cursor is out of range",
        ));
    }
    let mut page_staged = Vec::new();
    let mut page_worktree = Vec::new();
    let mut count = 0;
    // The envelope contains two refs (at most 512 bytes each), two OIDs,
    // the Agent binding and delimiters. Leave ample room for all of it.
    let mut encoded_bytes = 4096;
    for (index, item) in staged
        .iter()
        .chain(worktree.iter())
        .enumerate()
        .skip(after)
        .take(request.limit)
    {
        let encoded = serde_json::to_vec(item)
            .map_err(|_| Error::new("internal", "checkpoint metadata cannot be encoded"))?;
        encoded_bytes += encoded.len() + 1;
        if encoded_bytes > PAGE_BYTES {
            break;
        }
        if index < staged.len() {
            page_staged.push(item.clone());
        } else {
            page_worktree.push(item.clone());
        }
        count += 1;
    }
    if count == 0 && after < total {
        return Err(Error::new(
            "unavailable",
            "checkpoint metadata entry exceeds the response byte limit",
        ));
    }
    let next_after = after + count;
    let next = (next_after < total).then(|| CheckpointGetCursor {
        checkpoint_ref: request.checkpoint_ref.clone(),
        oid: request.oid.clone(),
        after: next_after,
    });
    Ok(CheckpointGetResponse {
        agent: request.agent.clone(),
        generation: request.generation,
        checkpoint_ref: request.checkpoint_ref.clone(),
        oid: request.oid.clone(),
        head: manifest.head,
        staged: page_staged,
        worktree: page_worktree,
        next,
    })
}
