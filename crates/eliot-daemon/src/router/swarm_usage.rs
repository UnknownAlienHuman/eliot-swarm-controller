use super::{Principal, Router};
use eliot_core::api::SwarmMcpToolStatus;

fn canonical_rpc_method(method: &str) -> bool {
    if method.is_empty() || method.len() > 256 {
        return false;
    }
    method.split('.').all(|segment| {
        let mut bytes = segment.bytes();
        matches!(bytes.next(), Some(b'a'..=b'z'))
            && bytes.all(|byte| matches!(byte, b'a'..=b'z' | b'0'..=b'9' | b'_'))
    })
}

pub(super) struct SwarmMcpCallTrace {
    agent: Option<(String, i64)>,
    call_id: String,
}

impl Router {
    pub(super) async fn begin_swarm_mcp_tool_call(
        &self,
        principal: &Principal,
        remote: Option<&eliot_core::RemoteCallContext>,
        call_id: &str,
        method: &str,
    ) -> Option<SwarmMcpCallTrace> {
        if ulid::Ulid::from_string(call_id).is_err()
            || !canonical_rpc_method(method)
            || self
                .secret_policy_snapshot()
                .contains_value(&serde_json::json!(method))
        {
            return None;
        }
        let agent = match (principal, remote) {
            (Principal::Agent { id, generation }, None) => Some((id.as_str(), *generation)),
            (Principal::Owner, Some(remote))
                if remote.call_id == call_id && remote.mcp_request_id.is_some() =>
            {
                None
            }
            _ => return None,
        };
        let mut store = self.inner.store.lock().await;
        let (tracked, turn_id, events) = store
            .record_swarm_mcp_tool_started(agent, call_id, method, crate::common::now_ms())
            .ok()?;
        if !tracked || (agent.is_some() && turn_id.is_none()) {
            return None;
        }
        self.publish_locked(&events);
        drop(store);
        Some(SwarmMcpCallTrace {
            agent: agent.map(|(id, generation)| (id.to_owned(), generation)),
            call_id: call_id.to_owned(),
        })
    }

    pub(super) async fn finish_swarm_mcp_tool_call(
        &self,
        trace: &SwarmMcpCallTrace,
        refused: bool,
    ) {
        let status = if refused {
            SwarmMcpToolStatus::Refused
        } else {
            SwarmMcpToolStatus::Completed
        };
        let mut store = self.inner.store.lock().await;
        if let Ok(events) = store.record_swarm_mcp_tool_completed(
            trace
                .agent
                .as_ref()
                .map(|(id, generation)| (id.as_str(), *generation)),
            &trace.call_id,
            status,
            crate::common::now_ms(),
        ) {
            self.publish_locked(&events);
        }
        drop(store);
    }
}
