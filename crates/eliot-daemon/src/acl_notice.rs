use crate::{bus::Bus, platform::ForeignDataDirAce};
use eliot_core::{SecretPolicy, api::NotifyDefinitionArgs, workspace_path_key};
use eliot_plugin::sync::LockExt;
use eliot_store::Store;
use std::{collections::HashSet, sync::Mutex as SyncMutex};
use tokio::sync::Mutex;

#[derive(Hash, Eq, PartialEq)]
struct GrantKey {
    path: String,
    sid: Option<String>,
    rights: Option<u32>,
}

/// Shared by startup, periodic checks, and managed workspace/plugin creation.
#[derive(Default)]
pub(crate) struct AclNotices {
    seen: SyncMutex<HashSet<GrantKey>>,
}

impl AclNotices {
    /// Report each distinct removal once per daemon run. Returns whether the
    /// observation included a grant already reported by an earlier repair.
    pub(crate) async fn report(
        &self,
        store: &Mutex<Store>,
        bus: &Bus,
        policy: &SecretPolicy,
        removed: Vec<ForeignDataDirAce>,
    ) -> bool {
        if removed.is_empty() {
            return false;
        }
        let (fresh, repeated) = {
            let mut seen = self.seen.lock_recover();
            let mut batch = HashSet::new();
            let mut fresh = Vec::new();
            let mut repeated = false;
            for ace in removed {
                let key = GrantKey {
                    path: workspace_path_key(&ace.path.to_string_lossy()).into_owned(),
                    sid: ace.sid.clone(),
                    rights: ace.rights,
                };
                if seen.contains(&key) {
                    repeated = true;
                } else if batch.insert(key) {
                    fresh.push(ace);
                }
            }
            seen.extend(batch);
            (fresh, repeated)
        };
        if fresh.is_empty() {
            return repeated;
        }
        let notices: Vec<_> = fresh
            .iter()
            .map(|ace| removal_notice(policy, ace))
            .collect();
        let mut store = store.lock().await;
        for notice in notices {
            match store.commit_acl_repair_notice(&notice, crate::common::now_ms()) {
                Ok(event) => bus.publish(&[event]),
                Err(error) => {
                    drop(store);
                    crate::logging::record(policy, "daemon.acl_notice_failed", &[], Some(&error));
                    break;
                }
            }
        }
        repeated
    }
}

fn removal_notice(policy: &SecretPolicy, ace: &ForeignDataDirAce) -> NotifyDefinitionArgs {
    let sid = ace.sid.as_deref().unwrap_or("unknown");
    let rights = ace
        .rights
        .map_or_else(|| "unknown".to_owned(), |rights| format!("0x{rights:08X}"));
    let path = ace.path.to_string_lossy();
    let operation = format!(
        "Removed ACL entry with rights {rights}, type 0x{:02X}, flags 0x{:02X}",
        ace.ace_type, ace.flags
    );
    crate::logging::record(
        policy,
        "daemon.data_dir_acl_repaired",
        &[
            ("principal", sid),
            ("path", path.as_ref()),
            ("operation", &operation),
        ],
        None,
    );
    NotifyDefinitionArgs {
        title: Some("Daemon-owned directory ACL repaired".to_owned()),
        message: policy.redact_text(&format!("{operation} for SID {sid} on {path:?}")),
    }
}
