//! Admission-only policy selection. Retained commands bypass this module.
use super::*;
use eliot_core::{RouteQuotaStatus, RouteSkip, RouteSkipReason, RoutingDecision};

#[derive(Default)]
struct RoutePreflight {
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
        let decision = match self.route_launch_profile(principal, &mut probe).await {
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
        let mut preflight = RoutePreflight::default();
        for name in candidates {
            let mut quota_status = RouteQuotaStatus::Unknown;
            let evaluated = self
                .routing_candidate(
                    principal,
                    request,
                    &name,
                    parent.as_ref(),
                    &mut quota_status,
                    &mut preflight,
                )
                .await?;
            match evaluated {
                Ok((harness, snapshot)) => {
                    decision.profile = Some(name.clone());
                    decision.reason = "first available candidate".into();
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

#[cfg(test)]
fn quota_exhausted(
    response: &eliot_core::api::HarnessQuotaResponse,
    model: Option<&str>,
    now: i64,
) -> bool {
    route_quota(
        &catalog::ScopedQuota {
            response: response.clone(),
            route_verified: true,
        },
        model,
        now,
    )
    .1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn role_selects_first_free_candidate_and_retains_decision_on_agent() {
        let root = std::env::temp_dir().join(format!("eliot-routing-{}", ulid::Ulid::new()));
        let _temp_1 = crate::test_support::TempDir::own(root.clone());
        std::fs::create_dir_all(&root).unwrap();
        let cmd = std::env::current_exe()
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/");
        std::fs::write(root.join("config.toml"), format!(
            "[routing]\nworker=['full','free']\n[profiles.full]\nharness='claude'\ncmd='{cmd}'\nmax_live=0\n[profiles.free]\nharness='claude'\ncmd='{cmd}'\n"
        )).unwrap();
        let router = super::super::tests::router_with_data_root(root.clone());
        let mut request = SubmitCommand::new(ulid::Ulid::new().to_string(), "launch");
        request.args = json!({"role":"worker","workspace":"w","project":"p"});
        router
            .freeze_launch_profile(&Principal::Owner, &mut request)
            .await
            .unwrap();
        assert_eq!(request.args["profile"], "free");
        assert_eq!(
            request.args["profile_choice"]["skipped"][0]["reason"],
            "capacity_full"
        );
        let mut store = router.inner.store.lock().await;
        let admission = store.submit("owner", request, now_ms()).unwrap();
        let agent = store
            .get_agent(admission.command.target.as_deref().unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(agent.profile_choice.unwrap().role, "worker");
        drop(store);
        drop(router);
    }

    #[tokio::test]
    async fn preview_reports_the_choice_and_misses_without_admitting() {
        let root = std::env::temp_dir().join(format!("eliot-routing-{}", ulid::Ulid::new()));
        let _temp_2 = crate::test_support::TempDir::own(root.clone());
        std::fs::create_dir_all(&root).unwrap();
        let cmd = std::env::current_exe()
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/");
        std::fs::write(root.join("config.toml"), format!(
            "[routing]\nworker=['full','free']\naudit=['full']\n[profiles.full]\nharness='claude'\ncmd='{cmd}'\nmax_live=0\n[profiles.free]\nharness='claude'\ncmd='{cmd}'\n"
        )).unwrap();
        let router = super::super::tests::router_with_data_root(root.clone());
        let chosen = router
            .routing_preview(&json!({"role":"worker"}), &Principal::Owner)
            .await
            .unwrap();
        assert_eq!(chosen["decision"]["profile"], "free");
        assert_eq!(chosen["decision"]["skipped"][0]["reason"], "capacity_full");
        let missed = router
            .routing_preview(&json!({"role":"audit"}), &Principal::Owner)
            .await
            .unwrap();
        assert!(missed["decision"]["profile"].is_null());
        assert_eq!(missed["decision"]["skipped"][0]["profile"], "full");
        assert!(
            router
                .inner
                .store
                .lock()
                .await
                .list_agents(None, 10)
                .unwrap()
                .0
                .is_empty()
        );
        drop(router);
    }

    #[tokio::test]
    async fn all_full_candidates_preserve_capacity_refusal_details() {
        let root = std::env::temp_dir().join(format!("eliot-routing-full-{}", ulid::Ulid::new()));
        let _temp = crate::test_support::TempDir::own(root.clone());
        std::fs::create_dir_all(&root).unwrap();
        let cmd = std::env::current_exe()
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/");
        std::fs::write(
            root.join("config.toml"),
            format!(
                "[routing]\nworker=['full']\n[profiles.full]\nharness='claude'\ncmd='{cmd}'\nmax_live=0\n"
            ),
        )
        .unwrap();
        let router = super::super::tests::router_with_data_root(root.clone());
        let mut request = SubmitCommand::new(ulid::Ulid::new().to_string(), "launch");
        request.args = json!({"role":"worker","workspace":"w","project":"p"});
        let error = router
            .freeze_launch_profile(&Principal::Owner, &mut request)
            .await
            .unwrap_err();
        assert_eq!(error.code, "capacity_unavailable");
        assert!(error.message.contains("profile:full"));
        assert_eq!(
            error.routing.as_ref().unwrap().skipped[0].reason,
            RouteSkipReason::CapacityFull
        );
    }

    #[test]
    fn held_account_identity_includes_ambient_native_store_selectors() {
        assert_ne!(
            account_identity(&json!({"pass_env_values":{"CODEX_HOME":"one"}})),
            account_identity(&json!({"pass_env_values":{"CODEX_HOME":"two"}}))
        );
        assert_eq!(
            account_identity(&json!({"model":"one"})),
            account_identity(&json!({"model":"two"}))
        );
        assert!(!exhausted(
            "codex",
            json!({"rateLimits":{"primary":{"usedPercent":100,"resetsAt":1}}}),
            None,
        ));
    }

    fn supported_quota(harness: &str, details: Value) -> eliot_core::api::HarnessQuotaResponse {
        eliot_core::api::HarnessQuotaResponse {
            harness: harness.to_owned(),
            scope: format!("profile:test harness:{harness}"),
            fetched_at_ms: 1_000_000,
            ttl_ms: 300_000,
            credential: "ambient".to_owned(),
            status: eliot_core::api::HARNESS_QUOTA_SUPPORTED.to_owned(),
            note: None,
            details: Some(details),
        }
    }

    fn exhausted(harness: &str, details: Value, model: Option<&str>) -> bool {
        quota_exhausted(&supported_quota(harness, details), model, 1_000_100)
    }
    #[test]
    fn quota_exhaustion_covers_retained_windows_and_all_adapters() {
        // Retained Muse `windows[]` observations skip the route like live reads.
        assert!(exhausted(
            "muse",
            json!({"windows": [{"label": "Weekly", "usedPercent": 100.0, "resetsAtMs": i64::MAX}]}),
            None,
        ));
        for harness in ["claude", "antigravity"] {
            assert!(
                exhausted(
                    harness,
                    json!({"windows": [{"label": "Weekly", "usedPercent": 100.0, "resetsAtMs": i64::MAX}]}),
                    None,
                ),
                "{harness} exhausted windows skip"
            );
            assert!(
                !exhausted(
                    harness,
                    json!({"windows": [{"label": "Weekly", "usedPercent": 10.0, "resetsAtMs": i64::MAX}]}),
                    None,
                ),
                "{harness} headroom does not skip"
            );
        }
        // A model-scoped window never skips an unrelated model: account quota
        // alone invents no per-model entitlement.
        assert!(!exhausted(
            "claude",
            json!({"windows": [{"label": "Opus weekly", "usedPercent": 100.0, "resetsAtMs": i64::MAX}]}),
            Some("sonnet-4"),
        ));
        // Unmapped shapes never skip.
        assert!(!exhausted("muse", json!({}), None));
        assert!(!exhausted("opencode", json!({"rateLimits": {}}), None));
    }

    #[test]
    fn quota_checks_only_known_native_windows_and_preserves_unknown() {
        assert!(exhausted(
            "codex",
            json!({"rateLimits":{"primary":{"usedPercent":100}}}),
            None,
        ));
        assert!(!exhausted(
            "codex",
            json!({"rateLimits":{"primary":{"usedPercent":100},"credits":{"hasCredits":true}}}),
            None,
        ));
        assert!(exhausted(
            "muse",
            json!({"usage":{"weekly":{"usedPercent":100}}}),
            None,
        ));
        assert!(!exhausted("muse", json!({}), None));
        assert!(!exhausted(
            "codex",
            json!({"rateLimitsByLimitId":{"other":{"primary":{"usedPercent":100}}}}),
            None,
        ));
    }

    #[test]
    fn stale_quota_observation_never_skips_a_route() {
        // An exhausted retained window past its freshness TTL stays unknown:
        // routing and the catalogue share the freshness gate, so an expired
        // observation with a future reset cannot skip a route.
        let stale = eliot_core::api::HarnessQuotaResponse {
            fetched_at_ms: 1_000,
            ttl_ms: 300_000,
            ..supported_quota(
                "muse",
                json!({"windows": [{"label": "Weekly", "usedPercent": 100.0, "resetsAtMs": i64::MAX}]}),
            )
        };
        assert!(!quota_exhausted(&stale, Some("m"), 1_000 + 300_000 + 1));
        assert!(quota_exhausted(&stale, Some("m"), 1_000 + 300_000));
        let scoped = catalog::ScopedQuota {
            response: stale,
            route_verified: true,
        };
        assert_eq!(
            route_quota(&scoped, Some("m"), 1_000 + 300_000 + 1),
            (RouteQuotaStatus::Unknown, false)
        );
    }
    #[test]
    fn codex_quota_uses_primary_limit_state_only() {
        // Audit #630: explicit backend limit state exhausts even with
        // healthy windows.
        assert!(exhausted(
            "codex",
            json!({"rateLimits":{"primary":{"usedPercent":10},"spendControlReached":true}}),
            None,
        ));
        assert!(exhausted(
            "codex",
            json!({"rateLimits":{"primary":{"usedPercent":10},"rateLimitReachedType":"rate_limit_reached"}}),
            None,
        ));
        assert!(exhausted(
            "codex",
            json!({"rateLimits":{"primary":{"usedPercent":10},"individualLimit":{"remainingPercent":0}}}),
            None,
        ));
        // Healthy limit state with healthy windows is not exhausted.
        assert!(!exhausted(
            "codex",
            json!({"rateLimits":{"primary":{"usedPercent":10},"spendControlReached":false,"individualLimit":{"remainingPercent":40}}}),
            None,
        ));
        // A full secondary window alone does not exhaust the primary route:
        // the request uses the primary bucket, not the secondary one.
        assert!(!exhausted(
            "codex",
            json!({"rateLimits":{"primary":{"usedPercent":10},"secondary":{"usedPercent":100}}}),
            None,
        ));
        // Metered buckets never exhaust a route, even when their projected
        // slug names the requested model: `normalModelSlug` only names
        // display and reasoning options, never the metered model.
        let details = json!({"rateLimits":{"primary":{"usedPercent":1}},
            "rateLimitsByLimitId":{"codex":{"primary":{"usedPercent":100},"normalModelSlug":"gpt-5.4"}}});
        assert!(!exhausted("codex", details, None));
        // Luna Reserve-style alias bucket: an exhausted Reserve allowance
        // must not skip ordinary Luna when ordinary quota is healthy.
        let reserve = json!({"rateLimits":{"primary":{"usedPercent":10}},
            "rateLimitsByLimitId":{"luna-reserve":{"primary":{"usedPercent":100},"normalModelSlug":"gpt-5.4","limitId":"luna-reserve"}}});
        assert!(!exhausted("codex", reserve, None));
    }
}
