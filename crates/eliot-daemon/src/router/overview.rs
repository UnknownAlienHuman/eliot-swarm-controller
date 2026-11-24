//! Project overview routing and its principal boundary.
use super::*;
use eliot_core::api::{ProjectOverviewRequest, discovery::decode_params};

impl Router {
    pub(super) async fn project_overview(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        let request: ProjectOverviewRequest = decode_params(params.clone())?;
        request.validate()?;
        if !matches!(principal, Principal::Owner | Principal::Agent { .. }) {
            return Err(Error::new(
                "not_allowed",
                "project overview requires Owner or Agent authority",
            ));
        }
        let limits = crate::config::load_scope_limits(&self.inner.data_root)?;
        let principal = principal_name(principal);
        let response = {
            let store = self.inner.store.lock().await;
            store.project_overview(&limits, &request, &principal, now_ms())?
        };
        serde_json::to_value(response)
            .map_err(|_| Error::new("internal", "project overview could not be encoded"))
    }
}
