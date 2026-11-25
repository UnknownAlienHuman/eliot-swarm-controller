//! Bounded validation in an owned adapter reader, before native dispatch.

use super::{Router, command::retained_resume_source, validate_retained_resume_subject};
use eliot_core::{Agent, Result, api::UpdateProbeKind};

impl Router {
    pub(super) async fn probe_retained_start(
        &self,
        agent: Agent,
        kind: UpdateProbeKind,
    ) -> Result<()> {
        let source_state = if kind == UpdateProbeKind::Resume {
            validate_retained_resume_subject(&agent)?;
            let mut snapshot = {
                let store = self.inner.store.lock().await;
                retained_resume_source(&store, &agent)?
            };
            self.redact_native_value(&mut snapshot);
            Some(snapshot)
        } else {
            None
        };
        self.inner
            .plugin_host
            .validate_retained_start(agent, source_state)
            .await
    }
}
