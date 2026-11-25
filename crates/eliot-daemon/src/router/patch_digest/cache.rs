//! The interactive digest reuses only Git's input projection. Agent authority,
//! attribution, claims and guarded review evidence still use their own reads.

use crate::{
    fs_invalidation::{FilesystemRevision, WorkspaceInvalidation},
    git_monitor::{GitProbe, PatchRead},
    monitor::Monitoring,
    workspace_git::{CheckoutReadStamp, DiffCheckout},
};
use eliot_core::Result;
use eliot_plugin::sync::LockExt;
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};
use tokio::sync::Mutex as AsyncMutex;

const PATCH_READBACK_CAPACITY: usize = 8;

#[derive(Default)]
pub(in crate::router) struct PatchReadCache {
    slots: Mutex<VecDeque<(String, Arc<AsyncMutex<PatchSlot>>)>>,
}

#[derive(Default)]
struct PatchSlot {
    fallback: Option<WorkspaceInvalidation>,
    attempted_watch: bool,
    cached: Option<CachedPatch>,
}

struct CachedPatch {
    proof: ReadProof,
    base: Option<String>,
    read: Arc<PatchRead>,
}

#[derive(PartialEq, Eq)]
struct ReadProof {
    revision: FilesystemRevision,
    stamp: CheckoutReadStamp,
}

impl PatchReadCache {
    fn slot(&self, workspace: &str) -> Arc<AsyncMutex<PatchSlot>> {
        let mut slots = self.slots.lock_recover();
        let slot = slots
            .iter()
            .position(|(path, _)| path == workspace)
            .and_then(|position| slots.remove(position))
            .map(|(_, slot)| slot)
            .unwrap_or_default();
        slots.push_back((workspace.to_owned(), Arc::clone(&slot)));
        let retired = (slots.len() > PATCH_READBACK_CAPACITY)
            .then(|| slots.pop_front())
            .flatten();
        drop(slots);
        // Dropping a watch can join the backend thread.
        if let Some(retired) = retired {
            tokio::task::spawn_blocking(move || drop(retired));
        }
        slot
    }

    pub(in crate::router) async fn read(
        &self,
        monitoring: &Monitoring,
        workspace: &str,
        common: Option<&str>,
        base: Option<&str>,
    ) -> Result<Arc<PatchRead>> {
        let slot = self.slot(workspace);
        let mut slot = slot.lock().await;
        if monitoring.git_watch_revision(workspace).is_none() && !slot.attempted_watch {
            slot.attempted_watch = true;
            let path = workspace.to_owned();
            let common = common.map(str::to_owned);
            // A retained inactive checkout has no process-monitor slot. Use
            // the same watcher engine for an admitted, bounded readback slot.
            slot.fallback = tokio::task::spawn_blocking(move || {
                WorkspaceInvalidation::watch(&path, common.as_deref()).ok()
            })
            .await
            .ok()
            .flatten();
        }
        let before = slot.proof(monitoring, workspace, common).await;
        if let (Some(proof), Some(cached)) = (before.as_ref(), slot.cached.as_ref())
            && cached.base.as_deref() == base
            && &cached.proof == proof
        {
            return Ok(Arc::clone(&cached.read));
        }
        // A changed or unverifiable proof cannot be used after a failed read.
        slot.cached = None;
        let read = Arc::new(GitProbe::new()?.patch_read(workspace, base).await?);
        if let Some(proof) = before
            && read.head.as_deref() == Some(proof.stamp.head.as_str())
            && slot.proof(monitoring, workspace, common).await.as_ref() == Some(&proof)
        {
            slot.cached = Some(CachedPatch {
                proof,
                base: base.map(str::to_owned),
                read: Arc::clone(&read),
            });
        }
        Ok(read)
    }
}

impl PatchSlot {
    fn revision(&self, monitoring: &Monitoring, workspace: &str) -> Option<FilesystemRevision> {
        monitoring
            .git_watch_revision(workspace)
            .or_else(|| self.fallback.as_ref()?.revision())
    }

    async fn proof(
        &self,
        monitoring: &Monitoring,
        workspace: &str,
        common: Option<&str>,
    ) -> Option<ReadProof> {
        let revision = self.revision(monitoring, workspace)?;
        let path = workspace.to_owned();
        let common = common.map(str::to_owned);
        let stamp = tokio::task::spawn_blocking(move || {
            DiffCheckout::open(&path, common.as_deref())?.read_stamp()
        })
        .await
        .ok()?
        .ok()?;
        (self.revision(monitoring, workspace).as_ref() == Some(&revision))
            .then_some(ReadProof { revision, stamp })
    }
}
