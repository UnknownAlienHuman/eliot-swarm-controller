use super::{MODE_OPEN, Principal, Router};
use crate::common::now_ms;
use eliot_core::Result;
use eliot_store::AgentRetirementPolicy;
use std::sync::atomic::Ordering;

impl Router {
    pub(crate) async fn retire_agent_page(
        &self,
        policy: &AgentRetirementPolicy,
        after: Option<i64>,
    ) -> Result<Option<i64>> {
        let Ok(_write) = self.inner.restart_writes.try_read() else {
            return Ok(after);
        };
        let mut store = self.inner.store.lock().await;
        if self.inner.mode.load(Ordering::SeqCst) != MODE_OPEN {
            return Ok(after);
        }
        let change = store.archive_retired_agents(policy, after, now_ms())?;
        self.publish_locked(&change.events);
        Ok(change.value)
    }

    pub(super) async fn note_child_result_read(
        &self,
        principal: &Principal,
        child: &str,
        child_generation: i64,
        turn_id: &str,
    ) {
        let Principal::Agent { id, generation } = principal else {
            return;
        };
        if id == child {
            return;
        }
        let Ok(_write) = self.inner.restart_writes.try_read() else {
            return;
        };
        let mut store = self.inner.store.lock().await;
        if self.inner.mode.load(Ordering::SeqCst) == MODE_OPEN
            && let Err(error) =
                store.record_child_result_read(id, *generation, child, child_generation, turn_id)
        {
            crate::logging::record(
                &self.secret_policy_snapshot(),
                "agent.result_read_evidence_failed",
                &[("agent", child), ("manager", id)],
                Some(&error),
            );
        }
    }
}
