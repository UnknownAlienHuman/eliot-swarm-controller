//! Admission-only policy selection. Retained commands bypass this module.
use super::*;
use crate::config::{
    QuotaUnknownPolicy, profile_quota_reserve_percent, profile_quota_unknown_policy,
};
use eliot_core::{RouteQuotaStatus, RouteSkip, RouteSkipReason, RoutingDecision};

#[derive(Clone, Copy, Default)]
pub(super) struct LaunchRouting<'a> {
    pub(super) arbitrated: bool,
    pub(super) owner_automation: bool,
    pub(super) provider_source: Option<&'a Value>,
    pub(super) preview: bool,
    pub(super) harness_check_candidate: Option<&'a eliot_core::api::HarnessCandidate>,
}

impl LaunchRouting<'_> {
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

/// Who may succeed a source whose successor no Owner chose by hand.
pub(super) struct HandoffRoute {
    pub(super) reason: eliot_core::api::HandoffReason,
    pub(super) kind: Option<eliot_core::ProviderRestrictionKind>,
    /// A role a manager asked for, in place of the source's own.
    pub(super) role: Option<String>,
    /// A manager's child profiles.
    pub(super) allowed: Option<Vec<String>>,
}

#[derive(Default)]
struct RoutePreflight {
    arbitrated: bool,
    automatic: bool,
    provider_switch: bool,
    offer_due_probe: bool,
    provider_authority: Option<[u8; 32]>,
    provider_source: Option<Value>,
    handoff_source: Option<Value>,
    handoff_allowed: Option<Vec<String>>,
    /// Source profile when a sandboxed successor could not commit here.
    handoff_no_commit: Option<String>,
    held_accounts: HashMap<String, Value>,
    capacity_refusal: Option<Error>,
    provider_refusal: Option<Error>,
    profile_command_refusal: Option<Error>,
    quota_policies: HashMap<String, QuotaPolicy>,
    discovery_deadline: Option<std::time::Instant>,
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
            .route_launch_profile(
                principal,
                &mut probe,
                LaunchRouting {
                    preview: true,
                    ..Default::default()
                },
            )
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
        mode: LaunchRouting<'_>,
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
            offer_due_probe: mode.preview
                || request.args["text"]
                    .as_str()
                    .is_some_and(|text| !text.trim().is_empty()),
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
    }

    pub(super) async fn check_explicit_launch_quota(
        &self,
        principal: &Principal,
        mode: LaunchRouting<'_>,
        profile: &str,
        harness: &str,
        snapshot: &Value,
    ) -> Result<()> {
        if !mode.automatic(principal) {
            return Ok(());
        }
        if self
            .inner
            .store
            .lock()
            .await
            .harness_check_blocks_automatic(harness)?
        {
            return Err(Error::new(
                "harness_check_failed",
                RouteSkipReason::HarnessCheckFailed.user_message(),
            ));
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
            provider_source: Some(source.profile_snapshot.clone()),
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

    /// Ordered successors: the role's candidates, else the fallback list. An
    /// explicit-profile Agent with no fallback list has none. A lost session
    /// may restart on its own profile; a held or failed one may not.
    pub(super) fn handoff_candidates(
        &self,
        source: &Agent,
        route: &HandoffRoute,
    ) -> Result<Vec<String>> {
        use eliot_core::api::HandoffReason;
        let routing = crate::config::load_routing_config(&self.inner.data_root)?;
        let role = route.role.as_deref().or_else(|| {
            source
                .profile_choice
                .as_ref()
                .map(|choice| choice.role.as_str())
        });
        let role_list = role
            .and_then(|role| routing.candidates(role))
            .unwrap_or_default();
        if role.is_none() && routing.handoff_fallback.is_empty() {
            return Ok(Vec::new());
        }
        let keep_source =
            route.reason == HandoffReason::SessionLost && source.state == AgentState::Lost;
        let tried: Vec<&str> = source
            .profile_choice
            .iter()
            .flat_map(|choice| &choice.skipped)
            .filter(|skip| skip.reason == RouteSkipReason::ProviderFailure)
            .map(|skip| skip.profile.as_str())
            .collect();
        let others = role_list.iter().any(|name| name != &source.profile);
        let mut list: Vec<String> = Vec::new();
        if keep_source {
            list.push(source.profile.clone());
        }
        list.extend(role_list.iter().cloned());
        if !others {
            list.extend(routing.handoff_fallback.iter().cloned());
        }
        let mut seen = HashSet::new();
        list.retain(|name| {
            seen.insert(name.clone())
                && (keep_source || name != &source.profile)
                && !tried.contains(&name.as_str())
        });
        Ok(list)
    }

    pub(super) async fn route_handoff(
        &self,
        request: &mut SubmitCommand,
        source: &Agent,
        route: &HandoffRoute,
    ) -> Result<(String, Value)> {
        use eliot_core::api::HandoffReason;
        let candidates = self.handoff_candidates(source, route)?;
        let role = route
            .role
            .clone()
            .or_else(|| {
                source
                    .profile_choice
                    .as_ref()
                    .map(|choice| choice.role.clone())
            })
            .unwrap_or_else(|| "handoff_fallback".to_owned());
        let mut skipped: Vec<RouteSkip> = source
            .profile_choice
            .iter()
            .flat_map(|choice| &choice.skipped)
            .filter(|skip| skip.reason == RouteSkipReason::ProviderFailure)
            .cloned()
            .collect();
        if route.reason != HandoffReason::SessionLost || source.state != AgentState::Lost {
            skipped.push(RouteSkip {
                profile: source.profile.clone(),
                reason: RouteSkipReason::ProviderFailure,
                quota_status: RouteQuotaStatus::Unknown,
            });
        }
        let reason = match route.reason {
            HandoffReason::ProviderHold => format!(
                "{} is on a provider {} hold",
                source.profile,
                route.kind.map_or("quota", |kind| kind.as_str())
            ),
            HandoffReason::SessionLost => format!("the {} session ended", source.profile),
            HandoffReason::Routing | HandoffReason::Request => {
                format!("the work moved from {}", source.profile)
            }
        };
        let decision = RoutingDecision {
            source: format!("settings.routing.{role}"),
            role,
            profile: None,
            reason,
            skipped,
            quota_status: RouteQuotaStatus::Unknown,
            provider_failover: None,
            owner_automation: true,
        };
        let can_commit = match source.workspace.as_deref() {
            Some(workspace) => self
                .inner
                .store
                .lock()
                .await
                .worktree_branch_base(workspace)?
                .is_some_and(|(branch, _)| branch.contains('/')),
            None => false,
        };
        let mut preflight = RoutePreflight {
            provider_switch: true,
            handoff_source: Some(source.profile_snapshot.clone()),
            handoff_allowed: route.allowed.clone(),
            handoff_no_commit: (!can_commit).then(|| source.profile.clone()),
            ..RoutePreflight::default()
        };
        // Never sidestep a held account through another account of the same
        // harness. Another harness is a separate, policy-named provider.
        if route.reason == HandoffReason::ProviderHold {
            preflight.held_accounts.insert(
                source.harness.clone(),
                switch_account_identity(&source.profile_snapshot),
            );
        }
        self.prepare_quota_policies(&mut preflight, &candidates, None);
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
        let rank_by_quota = !preflight.quota_policies.is_empty();
        preflight.discovery_deadline =
            Some(std::time::Instant::now() + crate::adhoc::DISCOVERY_BUDGET);
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
        // A failed self-check removes the harness from automatic routing until
        // a later check passes (#932 item 3). Explicit Owner launches bypass
        // routing, so they still work.
        if self
            .inner
            .store
            .lock()
            .await
            .harness_check_blocks_automatic(&harness)?
        {
            return Ok(Err(RouteSkipReason::HarnessCheckFailed));
        }
        if let Some(parent) = parent
            && hierarchy::inherit_child_launch_bounds(&mut snapshot, parent).is_err()
        {
            return Ok(Err(RouteSkipReason::Unauthorized));
        }
        let configured_pin = crate::adhoc::explicit_profile_command(&snapshot, &harness).is_some();
        let command_source = preflight.provider_source.as_ref().or_else(|| {
            parent
                .filter(|parent| parent.harness == harness)
                .map(|parent| &parent.profile_snapshot)
        });
        let command_bound = inherited_snapshot || command_source.is_some();
        if !inherited_snapshot
            && let Some(source) = command_source
            && crate::adhoc::inherit_profile_command(&mut snapshot, source, &harness).is_err()
        {
            return Ok(Err(RouteSkipReason::Unauthorized));
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
        if let Some(source) = &preflight.handoff_source
            && eliot_core::handoff_authority_refusal(source, &snapshot).is_some()
        {
            return Ok(Err(RouteSkipReason::WiderAuthority));
        }
        if preflight
            .handoff_no_commit
            .as_deref()
            .is_some_and(|source| source != name)
            && crate::work_truth::needs_commit_roots(&harness, &snapshot)
        {
            return Ok(Err(RouteSkipReason::CannotCommit));
        }
        if preflight
            .handoff_allowed
            .as_ref()
            .is_some_and(|allowed| !allowed.iter().any(|profile| profile == name))
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
        if preflight.check_candidate_holds() {
            let store = self.inner.store.lock().await;
            let probe_at = preflight.offer_due_probe.then(now_ms);
            if let Some(error) = store.routing_refusal(name, &harness, &snapshot, probe_at)? {
                return Ok(Err(preflight.record(error, harness, account)));
            }
        }
        if command_bound
            && configured_pin
            && let Some(cmd) = crate::adhoc::explicit_profile_command(&snapshot, &harness)
            && !crate::adhoc::profile_command_exists(cmd)
        {
            preflight.profile_command_refusal.get_or_insert_with(|| {
                crate::adhoc::missing_profile_command_refusal(name, &harness, cmd, None)
            });
            return Ok(Err(RouteSkipReason::MissingProfileCommand));
        }
        let deadline = preflight
            .discovery_deadline
            .ok_or_else(|| Error::new("internal", "routing has no discovery deadline"))?;
        if command_bound {
            if !native_installed(&harness, &snapshot) {
                return Ok(Err(RouteSkipReason::NotInstalled));
            }
        } else {
            let cmd = snapshot["cmd"].as_str();
            match crate::adhoc::resolve_profile_command_until(name, &harness, cmd, deadline).await {
                Ok(candidate) => {
                    crate::adhoc::write_profile_command_snapshot(&mut snapshot, candidate)?;
                }
                Err(error) => {
                    let refusal = eliot_core::AdmissionRefusal::of_parts(
                        &error.code,
                        error.failure_where.as_deref(),
                        error.origin,
                    );
                    if refusal == Some(eliot_core::AdmissionRefusal::ProfileCommand) {
                        preflight.profile_command_refusal.get_or_insert(error);
                        return Ok(Err(RouteSkipReason::MissingProfileCommand));
                    }
                    return Ok(Err(RouteSkipReason::NotInstalled));
                }
            }
        }
        let quota = if preflight.arbitrated && preflight.quota_policies.is_empty() {
            CandidateQuota {
                status: RouteQuotaStatus::Unknown,
                unmapped: false,
                exhausted: false,
                headroom: None,
            }
        } else {
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
            let probe_at = preflight.offer_due_probe.then(now_ms);
            if let Some(error) = store.routing_refusal(name, &harness, &snapshot, probe_at)? {
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
            return eliot_plugin::install::is_native_executable(std::path::Path::new(pin));
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
