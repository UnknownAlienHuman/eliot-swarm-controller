//! Admission-only policy selection. Retained commands bypass this module.
use super::*;
use crate::config::{
    QuotaUnknownPolicy, profile_quota_reserve_percent, profile_quota_unknown_policy,
};
use eliot_core::{RouteQuotaStatus, RouteSkip, RouteSkipReason, RoutingDecision};

#[derive(Clone, Copy, Default)]
pub(super) struct LaunchRouting {
    pub(super) arbitrated: bool,
    pub(super) owner_automation: bool,
}

impl LaunchRouting {
    fn automatic(self, principal: &Principal) -> bool {
        self.owner_automation
            || matches!(
                principal,
                Principal::Agent { .. } | Principal::ProjectRule { .. }
            )
    }
}

#[derive(Clone, Copy, Default)]
struct QuotaPolicy {
    reserve: u8,
    hold_unknown: bool,
}

impl QuotaPolicy {
    fn from_snapshot(snapshot: &Value) -> Self {
        Self {
            reserve: profile_quota_reserve_percent(snapshot),
            hold_unknown: profile_quota_unknown_policy(snapshot) == QuotaUnknownPolicy::Hold,
        }
    }

    fn enabled(self) -> bool {
        self.reserve > 0 || self.hold_unknown
    }

    fn refusal(self, quota: &CandidateQuota) -> Option<RouteSkipReason> {
        match quota
            .headroom
            .as_ref()
            .and_then(|headroom| headroom.max_used)
        {
            Some(used) if self.reserve > 0 && used > f64::from(100 - self.reserve) => {
                Some(RouteSkipReason::QuotaReserve)
            }
            None if self.hold_unknown => Some(RouteSkipReason::QuotaUnknown),
            _ => None,
        }
    }
}

pub(super) struct CandidateQuota {
    pub(super) status: RouteQuotaStatus,
    pub(super) unmapped: bool,
    exhausted: bool,
    headroom: Option<catalog::QuotaHeadroom>,
}

impl CandidateQuota {
    fn weekly_used(&self) -> Option<f64> {
        self.headroom
            .as_ref()
            .and_then(|headroom| headroom.weekly_used)
    }
}

struct AvailableRoute {
    name: String,
    harness: String,
    snapshot: Value,
    quota: CandidateQuota,
}

#[derive(Default)]
struct RoutePreflight {
    arbitrated: bool,
    automatic: bool,
    provider_switch: bool,
    provider_authority: Option<[u8; 32]>,
    held_accounts: HashMap<String, Value>,
    capacity_refusal: Option<Error>,
    provider_refusal: Option<Error>,
    profile_command_refusal: Option<Error>,
    quota_policies: HashMap<String, QuotaPolicy>,
    quota_deadline: Option<std::time::Instant>,
}

impl RoutePreflight {
    fn record(&mut self, refusal: Error, harness: String, account: Value) -> RouteSkipReason {
        if refusal.code == "capacity_unavailable" {
            self.capacity_refusal.get_or_insert(refusal);
            RouteSkipReason::CapacityFull
        } else {
            self.held_accounts.insert(harness, account);
            self.provider_refusal.get_or_insert(refusal);
            RouteSkipReason::ProviderHold
        }
    }

    fn check_candidate_holds(&self) -> bool {
        !self.arbitrated || self.automatic
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
            .route_launch_profile(principal, &mut probe, LaunchRouting::default())
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
        mode: LaunchRouting,
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
        let routing = crate::config::load_routing_config(&self.inner.data_root)?;
        let provider_reactions = self.inner.attention.provider_refusal.enabled;
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
        let candidates = routing.candidates(&role).unwrap_or_default().to_vec();
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
        let decision = RoutingDecision {
            role: role.clone(),
            profile: None,
            source: format!("settings.routing.{role}"),
            reason: "no available candidate".into(),
            skipped: Vec::new(),
            quota_status: RouteQuotaStatus::Unknown,
            provider_failover: (provider_reactions
                && routing.provider_failover()
                && matches!(principal, Principal::Owner)
                && !mode.arbitrated
                && request.args.get("chat") != Some(&Value::Bool(true)))
            .then(Vec::new),
            owner_automation: mode.owner_automation,
        };
        // A held account is never replaced with another account of the same
        // harness. Policy may still select a different native harness.
        let mut preflight = RoutePreflight {
            arbitrated: mode.arbitrated,
            automatic: mode.automatic(principal),
            ..RoutePreflight::default()
        };
        if preflight.automatic {
            self.prepare_quota_policies(&mut preflight, &candidates, parent.as_ref());
        }
        self.choose_route(
            principal,
            request,
            candidates,
            parent.as_ref(),
            decision,
            preflight,
        )
        .await
    }

    fn launch_quota_policy(&self, profile: &str, inherited: Option<&Value>) -> QuotaPolicy {
        match self.resolve_profile_cached(profile) {
            Ok(snapshot) => QuotaPolicy::from_snapshot(&snapshot),
            Err(_) => inherited
                .map(QuotaPolicy::from_snapshot)
                .unwrap_or_default(),
        }
    }

    fn prepare_quota_policies(
        &self,
        preflight: &mut RoutePreflight,
        candidates: &[String],
        parent: Option<&Agent>,
    ) {
        preflight.quota_policies = candidates
            .iter()
            .filter_map(|name| {
                let inherited = parent.filter(|parent| parent.profile == *name);
                let policy = self
                    .launch_quota_policy(name, inherited.map(|parent| &parent.profile_snapshot));
                policy.enabled().then(|| (name.clone(), policy))
            })
            .collect();
        if !preflight.quota_policies.is_empty() {
            preflight.quota_deadline =
                Some(std::time::Instant::now() + crate::adhoc::DISCOVERY_BUDGET);
        }
    }

    pub(super) async fn check_explicit_launch_quota(
        &self,
        principal: &Principal,
        mode: LaunchRouting,
        profile: &str,
        harness: &str,
        snapshot: &Value,
    ) -> Result<()> {
        if !mode.automatic(principal) {
            return Ok(());
        }
        let policy = self.launch_quota_policy(profile, Some(snapshot));
        if !policy.enabled() {
            return Ok(());
        }
        let deadline = std::time::Instant::now() + crate::adhoc::DISCOVERY_BUDGET;
        let quota = self
            .read_candidate_quota(profile, harness, snapshot, deadline)
            .await;
        let refusal = policy.refusal(&quota);
        if quota.exhausted && refusal != Some(RouteSkipReason::QuotaReserve) {
            return Err(Error::new(
                "provider_held",
                format!("{profile}: quota used up"),
            ));
        }
        if let Some(reason) = refusal {
            let message = match (
                reason,
                quota
                    .headroom
                    .as_ref()
                    .and_then(|headroom| headroom.max_used),
            ) {
                (RouteSkipReason::QuotaReserve, Some(used)) => format!(
                    "{profile}: provider window usage {used:.2}% exceeds the automatic launch threshold of {}%",
                    100 - policy.reserve,
                ),
                _ => format!("{profile}: {}", reason.user_message()),
            };
            return Err(Error::new(quota_wait_code(reason), message));
        }
        Ok(())
    }

    pub(super) async fn route_provider_switch(
        &self,
        request: &mut SubmitCommand,
        source: &Agent,
        kind: eliot_core::ProviderRestrictionKind,
        authority: [u8; 32],
        owner_automation: bool,
    ) -> Result<(String, Value)> {
        if !self.inner.attention.provider_refusal.enabled {
            return Err(Error::new(
                "not_allowed",
                "automatic provider reactions are disabled",
            ));
        }
        let mut decision = source.profile_choice.clone().ok_or_else(|| {
            Error::new("source_integrity", "provider switch has no retained route")
        })?;
        let candidates = decision.provider_failover.take().ok_or_else(|| {
            Error::new(
                "source_integrity",
                "provider switch has no remaining policy",
            )
        })?;
        decision.profile = None;
        decision.reason = format!("{} refused the task ({})", source.profile, kind.as_str());
        decision.skipped.push(RouteSkip {
            profile: source.profile.clone(),
            reason: RouteSkipReason::ProviderFailure,
            quota_status: decision.quota_status,
        });
        decision.quota_status = RouteQuotaStatus::Unknown;
        decision.provider_failover = Some(Vec::new());
        decision.owner_automation |= owner_automation;
        let mut preflight = RoutePreflight {
            provider_switch: true,
            provider_authority: Some(authority),
            ..RoutePreflight::default()
        };
        preflight.held_accounts.insert(
            source.harness.clone(),
            switch_account_identity(&source.profile_snapshot),
        );
        if decision.owner_automation {
            self.prepare_quota_policies(&mut preflight, &candidates, None);
        }
        let (name, _, snapshot) = self
            .choose_route(
                &Principal::Owner,
                request,
                candidates,
                None,
                decision,
                preflight,
            )
            .await?;
        Ok((name, switch_account_identity(&snapshot)))
    }

    async fn choose_route(
        &self,
        principal: &Principal,
        request: &mut SubmitCommand,
        candidates: Vec<String>,
        parent: Option<&Agent>,
        mut decision: RoutingDecision,
        mut preflight: RoutePreflight,
    ) -> Result<(String, String, Value)> {
        let rank_by_quota = preflight.quota_deadline.is_some();
        let mut failover_candidates =
            (rank_by_quota && decision.provider_failover.is_some()).then(Vec::new);
        let mut selected: Option<AvailableRoute> = None;
        let mut candidates = candidates.into_iter();
        for name in candidates.by_ref() {
            let mut quota_status = RouteQuotaStatus::Unknown;
            let evaluated = self
                .routing_candidate(
                    principal,
                    request,
                    &name,
                    parent,
                    &mut quota_status,
                    &mut preflight,
                )
                .await?;
            match evaluated {
                Ok(candidate) => {
                    if let Some(tail) = &mut failover_candidates {
                        tail.push(candidate.name.clone());
                    }
                    if selected.as_ref().is_none_or(|prior| {
                        candidate.quota.weekly_used().is_some_and(|used| {
                            prior.quota.weekly_used().is_none_or(|first| used < first)
                        })
                    }) {
                        selected = Some(candidate);
                    }
                    if !rank_by_quota {
                        break;
                    }
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
        if let Some(AvailableRoute {
            name,
            harness,
            snapshot,
            quota,
        }) = selected
        {
            decision.profile = Some(name.clone());
            decision.reason = if preflight.provider_switch {
                format!("{}. Continuing on {name}", decision.reason)
            } else if rank_by_quota && quota.weekly_used().is_some() {
                "lowest observed weekly quota usage among available candidates".into()
            } else if preflight.arbitrated {
                "first policy candidate for the selected project".into()
            } else {
                "first available candidate".into()
            };
            if decision.provider_failover.is_some() {
                decision.provider_failover = Some(match failover_candidates {
                    Some(candidates) => candidates
                        .into_iter()
                        .filter(|route| route != &name)
                        .collect(),
                    None => candidates.collect(),
                });
            }
            decision.quota_status = quota.status;
            request.args["profile_choice"] = serde_json::to_value(decision)
                .map_err(|_| Error::new("internal", "routing decision encoding failed"))?;
            return Ok((name, harness, snapshot));
        }
        let quota_wait = decision
            .skipped
            .iter()
            .map(|skip| skip.reason)
            .find(|reason| {
                matches!(
                    reason,
                    RouteSkipReason::QuotaReserve | RouteSkipReason::QuotaUnknown
                )
            });
        let wait = preflight.capacity_refusal.or_else(|| {
            (preflight.arbitrated && quota_wait.is_none())
                .then_some(preflight.provider_refusal)
                .flatten()
        });
        let routing_error = Error::new(
            quota_wait.map_or("routing_unavailable", quota_wait_code),
            serde_json::to_string(&decision)
                .map_err(|_| Error::new("internal", "routing refusal encoding failed"))?,
        );
        let mut error = wait
            .or(preflight.profile_command_refusal)
            .unwrap_or(routing_error);
        error.routing = Some(Box::new(decision));
        Err(error)
    }

    async fn routing_candidate(
        &self,
        principal: &Principal,
        request: &SubmitCommand,
        name: &str,
        parent: Option<&Agent>,
        quota_status: &mut RouteQuotaStatus,
        preflight: &mut RoutePreflight,
    ) -> Result<std::result::Result<AvailableRoute, RouteSkipReason>> {
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
        let inherited_snapshot = parent.is_some_and(|parent| parent.profile == name);
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
        if let Some(parent) = parent
            && hierarchy::inherit_child_launch_bounds(&mut snapshot, parent).is_err()
        {
            return Ok(Err(RouteSkipReason::Unauthorized));
        }
        if !inherited_snapshot {
            let configured_cmd = snapshot
                .get("cmd")
                .and_then(Value::as_str)
                .map(str::to_owned);
            match crate::adhoc::resolve_profile_command(name, &harness, configured_cmd.as_deref())
                .await
            {
                Ok(candidate) => {
                    crate::adhoc::write_profile_command_snapshot(&mut snapshot, candidate)?;
                }
                Err(error) => {
                    if let Some(cmd) = configured_cmd.as_deref()
                        && !(harness == "claude" && crate::adhoc::is_latest_claude_selector(cmd))
                        && !crate::adhoc::profile_command_exists(cmd)
                    {
                        preflight.profile_command_refusal.get_or_insert(error);
                    }
                    return Ok(Err(RouteSkipReason::NotInstalled));
                }
            }
        }
        if parent.is_some() {
            let mut candidate = request.clone();
            candidate.args["profile"] = Value::String(name.to_owned());
            install_frozen_profile(&mut candidate, &harness, snapshot.clone(), true)?;
            let store = self.inner.store.lock().await;
            if hierarchy::check_agent_launch_authority(&store, principal, &candidate).is_err() {
                return Ok(Err(RouteSkipReason::Unauthorized));
            }
        }
        if let Some(authority) = preflight.provider_authority
            && eliot_core::provider_switch_authority(&snapshot)? != authority
        {
            return Ok(Err(RouteSkipReason::Unauthorized));
        }
        let account = if preflight.provider_switch {
            switch_account_identity(&snapshot)
        } else {
            account_identity(&snapshot)
        };
        if preflight
            .held_accounts
            .get(&harness)
            .is_some_and(|held| held != &account)
        {
            return Ok(Err(RouteSkipReason::Unauthorized));
        }
        if inherited_snapshot && !native_installed(&harness, &snapshot) {
            return Ok(Err(RouteSkipReason::NotInstalled));
        }
        if preflight.check_candidate_holds() {
            let store = self.inner.store.lock().await;
            if let Some(error) = store.routing_refusal(name, &harness, &snapshot)? {
                return Ok(Err(preflight.record(error, harness, account)));
            }
        }
        let quota = if preflight.arbitrated && preflight.quota_deadline.is_none() {
            CandidateQuota {
                status: RouteQuotaStatus::Unknown,
                unmapped: false,
                exhausted: false,
                headroom: None,
            }
        } else {
            let deadline = preflight
                .quota_deadline
                .unwrap_or_else(|| std::time::Instant::now() + crate::adhoc::DISCOVERY_BUDGET);
            self.read_candidate_quota(name, &harness, &snapshot, deadline)
                .await
        };
        *quota_status = quota.status;
        if let Some(reason) = preflight
            .quota_policies
            .get(name)
            .and_then(|policy| policy.refusal(&quota))
        {
            return Ok(Err(reason));
        }
        if preflight.check_candidate_holds() && quota.exhausted {
            preflight.held_accounts.insert(harness, account);
            preflight.provider_refusal.get_or_insert_with(|| {
                Error::new("provider_held", "routing candidate is quota exhausted")
            });
            return Ok(Err(RouteSkipReason::QuotaExhausted));
        }
        // Passive reads can take time. Recheck live state before freezing.
        if preflight.check_candidate_holds() {
            let store = self.inner.store.lock().await;
            if let Some(error) = store.routing_refusal(name, &harness, &snapshot)? {
                return Ok(Err(preflight.record(error, harness, account)));
            }
        }
        Ok(Ok(AvailableRoute {
            name: name.to_owned(),
            harness,
            snapshot,
            quota,
        }))
    }

    async fn read_candidate_quota(
        &self,
        profile: &str,
        harness: &str,
        snapshot: &Value,
        deadline: std::time::Instant,
    ) -> CandidateQuota {
        let evidence = self
            .quota_for_snapshot(
                harness.to_owned(),
                Some(snapshot.clone()),
                profile.to_owned(),
                deadline,
            )
            .await
            .ok();
        route_quota(evidence.as_ref(), snapshot, now_ms())
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

pub(super) fn switch_account_identity(snapshot: &Value) -> Value {
    let mut identity = account_identity(snapshot);
    // Compare the frozen selectors. The effective store binding is captured
    // at dispatch and exists on the source but not the prospective route.
    identity.as_object_mut().unwrap().remove("native_store");
    identity
}

fn native_installed(harness: &str, snapshot: &Value) -> bool {
    if harness == "claude" {
        if let Some(pin) = snapshot
            .get("cmd")
            .and_then(Value::as_str)
            .filter(|cmd| !crate::adhoc::is_latest_claude_selector(cmd))
        {
            return crate::adhoc::is_native_executable(std::path::Path::new(pin));
        }
        return crate::adhoc::claude_candidate_available();
    }
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

pub(super) fn route_quota(
    quota: Option<&catalog::ScopedQuota>,
    snapshot: &Value,
    now: i64,
) -> CandidateQuota {
    let model = snapshot["model"]
        .as_str()
        .or_else(|| snapshot["daemon_model"].as_str());
    if let Some(projection) = quota.and_then(|quota| catalog::projected_quota(quota, model, now)) {
        if projection.mapping == catalog::QuotaMapping::Unmapped {
            return CandidateQuota {
                status: RouteQuotaStatus::Unknown,
                unmapped: true,
                exhausted: false,
                headroom: None,
            };
        }
        return CandidateQuota {
            status: if projection.exhausted || projection.headroom.max_used.is_some() {
                RouteQuotaStatus::Supported
            } else {
                RouteQuotaStatus::Unknown
            },
            exhausted: projection.exhausted,
            unmapped: false,
            headroom: Some(projection.headroom),
        };
    }
    let status = if quota.is_some_and(|quota| quota.response.status == "unsupported") {
        RouteQuotaStatus::Unsupported
    } else {
        RouteQuotaStatus::Unknown
    };
    CandidateQuota {
        status,
        unmapped: false,
        exhausted: false,
        headroom: None,
    }
}

fn quota_wait_code(reason: RouteSkipReason) -> &'static str {
    match reason {
        RouteSkipReason::QuotaReserve => "quota_reserve",
        RouteSkipReason::QuotaUnknown => "quota_unknown",
        _ => "routing_unavailable",
    }
}
