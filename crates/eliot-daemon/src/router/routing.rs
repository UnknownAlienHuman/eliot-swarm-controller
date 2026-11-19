//! Admission-only policy selection. Retained commands bypass this module.
use super::*;
use eliot_core::{RouteQuotaStatus, RouteSkip, RouteSkipReason, RoutingDecision};

#[derive(Default)]
struct RoutePreflight {
    arbitrated: bool,
    held_accounts: HashMap<String, Value>,
    capacity_refusal: Option<Error>,
}

impl RoutePreflight {
    fn record(&mut self, refusal: Error, harness: String, account: Value) -> RouteSkipReason {
        if refusal.code == "capacity_unavailable" {
            self.capacity_refusal.get_or_insert(refusal);
            RouteSkipReason::CapacityFull
        } else {
            self.held_accounts.insert(harness, account);
            RouteSkipReason::ProviderHold
        }
    }
}

impl Router {
    /// `routing.preview`: run the admission selection for `role` without a
    /// command. Nothing is admitted, frozen or reserved; the next launch
    /// evaluates the candidates again and may decide differently.
    pub(super) async fn routing_preview(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        require_owner(principal)?;
        let request: eliot_core::api::RoutingPreviewRequest =
            eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let mut probe = SubmitCommand::new(String::new(), "launch");
        if let Some(role) = &request.role {
            probe.args = json!({ "role": role });
        }
        let decision = match self
            .route_launch_profile(principal, &mut probe, false)
            .await
        {
            Ok(_) => serde_json::from_value(probe.args["profile_choice"].take())
                .map_err(|_| Error::new("internal", "routing preview decision is malformed"))?,
            Err(error) if error.routing.is_some() => *error
                .routing
                .ok_or_else(|| Error::new("internal", "routing refusal lost its decision"))?,
            Err(error) => return Err(error),
        };
        let reply = eliot_core::api::RoutingPreviewResponse { decision };
        request.validate_response(&reply)?;
        serde_json::to_value(reply)
            .map_err(|_| Error::new("internal", "routing preview could not be encoded"))
    }

    pub(super) async fn route_launch_profile(
        &self,
        principal: &Principal,
        request: &mut SubmitCommand,
        arbitrated: bool,
    ) -> Result<(String, String, Value)> {
        let role = request
            .args
            .get("role")
            .map(|role| {
                role.as_str()
                    .ok_or_else(|| Error::new("invalid", "routing role must be a name"))
            })
            .transpose()?
            .unwrap_or("default")
            .to_owned();
        crate::config::validate_routing_role(&role)?;
        let parent = if matches!(principal, Principal::Agent { .. }) {
            let store = self.inner.store.lock().await;
            let id = required_text(&request.args, "parent")?;
            Some(
                store
                    .get_agent(id)?
                    .ok_or_else(|| Error::new("not_found", "routing parent disappeared"))?,
            )
        } else {
            None
        };
        let remote_registration = if matches!(principal, Principal::RemoteManager { .. }) {
            let store = self.inner.store.lock().await;
            let project = super::remote_manager::remote_project(&store, principal)?
                .ok_or_else(|| Error::new("internal", "remote launch without a project"))?;
            store.get_project_registration("owner", &project)?
        } else {
            None
        };
        let candidates = crate::config::routing_candidates(&self.inner.data_root, &role)?;
        // An unconfigured child default preserves the established inherited
        // profile path. An explicitly configured role is always intersected.
        let candidates = if candidates.is_empty()
            && role == "default"
            && let Some(parent) = parent.as_ref()
        {
            vec![parent.profile.clone()]
        } else {
            candidates
        };
        let mut decision = RoutingDecision {
            role: role.clone(),
            profile: None,
            source: format!("settings.routing.{role}"),
            reason: "no available candidate".into(),
            skipped: Vec::new(),
            quota_status: RouteQuotaStatus::Unknown,
        };
        // A held account is never replaced with another account of the same
        // harness. Policy may still select a different native harness.
        let mut preflight = RoutePreflight {
            arbitrated,
            ..RoutePreflight::default()
        };
        for name in candidates {
            let mut quota_status = RouteQuotaStatus::Unknown;
            let evaluated = self
                .routing_candidate(
                    principal,
                    request,
                    &name,
                    parent.as_ref(),
                    remote_registration.as_ref(),
                    &mut quota_status,
                    &mut preflight,
                )
                .await?;
            match evaluated {
                Ok((harness, snapshot)) => {
                    decision.profile = Some(name.clone());
                    decision.reason = if arbitrated {
                        "first policy candidate for the selected project"
                    } else {
                        "first available candidate"
                    }
                    .into();
                    decision.quota_status = quota_status;
                    request.args["profile_choice"] = serde_json::to_value(decision)
                        .map_err(|_| Error::new("internal", "routing decision encoding failed"))?;
                    return Ok((name, harness, snapshot));
                }
                Err(reason) => {
                    decision.skipped.push(RouteSkip {
                        profile: name,
                        reason,
                        quota_status,
                    });
                }
            }
        }
        let mut error = match preflight.capacity_refusal {
            Some(error) => error,
            None => Error::new(
                "routing_unavailable",
                serde_json::to_string(&decision)
                    .map_err(|_| Error::new("internal", "routing refusal encoding failed"))?,
            ),
        };
        error.routing = Some(Box::new(decision));
        Err(error)
    }

    async fn routing_candidate(
        &self,
        principal: &Principal,
        request: &SubmitCommand,
        name: &str,
        parent: Option<&Agent>,
        remote_registration: Option<&eliot_core::api::ProjectRegistrationView>,
        quota_status: &mut RouteQuotaStatus,
        preflight: &mut RoutePreflight,
    ) -> Result<std::result::Result<(String, Value), RouteSkipReason>> {
        if let Some(parent) = parent
            && name != parent.profile
            && !parent
                .profile_snapshot
                .get("child_profiles")
                .and_then(Value::as_array)
                .is_some_and(|allowed| {
                    allowed
                        .iter()
                        .any(|name_value| name_value.as_str() == Some(name))
                })
        {
            return Ok(Err(RouteSkipReason::Unauthorized));
        }
        if let Principal::ProjectRule { registration } = principal
            && !registration
                .profiles
                .iter()
                .any(|approved| approved == name)
        {
            return Ok(Err(RouteSkipReason::Unauthorized));
        }
        if let Some(registration) = remote_registration
            && !registration
                .profiles
                .iter()
                .any(|approved| approved == name)
        {
            return Ok(Err(RouteSkipReason::Unauthorized));
        }
        let mut snapshot = if let Some(parent) = parent.filter(|parent| parent.profile == name) {
            parent.profile_snapshot.clone()
        } else {
            match self.resolve_profile_cached(name) {
                Ok(mut snapshot) => {
                    if crate::config::capture_native_store_inputs(&mut snapshot, |name| {
                        std::env::var_os(name)
                    })
                    .is_err()
                    {
                        return Ok(Err(RouteSkipReason::InvalidProfile));
                    }
                    snapshot
                }
                Err(_) => return Ok(Err(RouteSkipReason::InvalidProfile)),
            }
        };
        let harness = required_text(&snapshot, "harness")?.to_owned();
        if let Some(parent) = parent {
            if hierarchy::inherit_child_launch_bounds(&mut snapshot, parent).is_err() {
                return Ok(Err(RouteSkipReason::Unauthorized));
            }
            let mut candidate = request.clone();
            candidate.args["profile"] = Value::String(name.to_owned());
            install_frozen_profile(&mut candidate, &harness, snapshot.clone(), true)?;
            let store = self.inner.store.lock().await;
            if hierarchy::check_agent_launch_authority(&store, principal, &candidate).is_err() {
                return Ok(Err(RouteSkipReason::Unauthorized));
            }
        }
        let account = account_identity(&snapshot);
        if preflight
            .held_accounts
            .get(&harness)
            .is_some_and(|held| held != &account)
        {
            return Ok(Err(RouteSkipReason::Unauthorized));
        }
        if !native_installed(&harness, &snapshot) {
            return Ok(Err(RouteSkipReason::NotInstalled));
        }
        if preflight.arbitrated {
            return Ok(Ok((harness, snapshot)));
        }
        {
            let store = self.inner.store.lock().await;
            if let Some(error) = store.routing_refusal(name, &harness, &snapshot)? {
                return Ok(Err(preflight.record(error, harness, account)));
            }
        }
        // Every harness with an implemented quota reader is probed through
        // the one normalized projection, so retained Muse windows and other
        // adapters' limits skip routes exactly like live reads. Unsupported
        // readers keep `Unknown`/`Unsupported` without skipping.
        let deadline = std::time::Instant::now() + crate::adhoc::DISCOVERY_BUDGET;
        if let Ok(evidence) = self
            .quota_for_snapshot(
                harness.clone(),
                Some(snapshot.clone()),
                name.to_owned(),
                deadline,
            )
            .await
        {
            let (status, exhausted) = route_quota(
                &evidence,
                snapshot.get("model").and_then(Value::as_str),
                now_ms(),
            );
            *quota_status = status;
            if exhausted {
                preflight.held_accounts.insert(harness, account);
                return Ok(Err(RouteSkipReason::QuotaExhausted));
            }
        }
        // Passive reads can take time. Recheck live state before freezing.
        let store = self.inner.store.lock().await;
        if let Some(error) = store.routing_refusal(name, &harness, &snapshot)? {
            return Ok(Err(preflight.record(error, harness, account)));
        }
        Ok(Ok((harness, snapshot)))
    }
}

fn account_identity(snapshot: &Value) -> Value {
    json!({"account":snapshot.get("account"), "native_store":snapshot.get("native_store_binding"),
        "native_route_identity":snapshot.get("native_route_identity"),
        "codex_home":snapshot.pointer("/pass_env_values/CODEX_HOME"),
        "codex_sqlite_home":snapshot.pointer("/pass_env_values/CODEX_SQLITE_HOME"),
        "integration":snapshot.get("auth_integration"), "route":snapshot.get("route_binding"),
        "pass_env":snapshot.get("pass_env"), "api_key_env":snapshot.get("api_key_env"),
        "literal_env":snapshot.get("literal_env")})
}

fn native_installed(harness: &str, snapshot: &Value) -> bool {
    let cmd = snapshot
        .get("cmd")
        .and_then(Value::as_str)
        .unwrap_or(harness);
    let path = std::path::Path::new(cmd);
    if path.is_absolute() || path.components().count() > 1 {
        return path.is_file();
    }
    std::env::var_os("PATH").is_some_and(|paths| {
        std::env::split_paths(&paths).any(|dir| {
            dir.join(cmd).is_file() || cfg!(windows) && dir.join(format!("{cmd}.exe")).is_file()
        })
    })
}

fn route_quota(
    quota: &catalog::ScopedQuota,
    model: Option<&str>,
    now: i64,
) -> (RouteQuotaStatus, bool) {
    if let Some(projection) = catalog::projected_quota(quota, model, now)
        && projection.mapping != catalog::QuotaMapping::Unmapped
    {
        return (RouteQuotaStatus::Supported, projection.exhausted);
    }
    let status = if quota.response.status == "unsupported" {
        RouteQuotaStatus::Unsupported
    } else {
        RouteQuotaStatus::Unknown
    };
    (status, false)
}
