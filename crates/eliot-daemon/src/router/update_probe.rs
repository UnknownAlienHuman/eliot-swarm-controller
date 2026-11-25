//! Read-only validation of the same retained inputs used for a managed start.

use super::{Router, plugin_host, validate_retained_resume_subject};
use eliot_core::{Agent, Error, FailureOrigin, Result, api::UpdateProbeKind};

impl Router {
    pub(super) fn probe_retained_start(&self, agent: &Agent, kind: UpdateProbeKind) -> Result<()> {
        plugin_host::validate_managed_start_profile(agent)?;
        if kind == UpdateProbeKind::Resume {
            validate_retained_resume_subject(agent)?;
        }
        if !eliot_core::api::is_supported_harness(&agent.harness) {
            return Err(Error::new(
                "implementation_gate",
                "managed start adapter is unsupported",
            )
            .with_origin(FailureOrigin::Swarm, "local adapter availability"));
        }
        Ok(())
    }
}
