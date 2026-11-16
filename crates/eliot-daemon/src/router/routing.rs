//! Admission-only policy selection. Retained commands bypass this module.
use super::*;
use eliot_core::{RouteQuotaStatus, RouteSkip, RouteSkipReason, RoutingDecision};

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
            Err(error) if error.code == "routing_unavailable" => *error
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
        let mut held_accounts = HashMap::<String, Value>::new();
        for name in candidates {
            let mut quota_status = RouteQuotaStatus::Unknown;
            let evaluated = self
                .routing_candidate(
                    principal,
                    request,
                    &name,
                    parent.as_ref(),
                    &mut quota_status,
                    &mut held_accounts,
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
                Err(reason) => decision.skipped.push(RouteSkip {
                    profile: name,
                    reason,
                    quota_status,
                }),
            }
        }
        // The refusal code is closed and the bounded JSON message carries the
        // same typed per-candidate decision as an accepted launch.
        let mut error = Error::new(
            "routing_unavailable",
            serde_json::to_string(&decision)
                .map_err(|_| Error::new("internal", "routing refusal encoding failed"))?,
        );
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
        held_accounts: &mut HashMap<String, Value>,
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
            match crate::config::resolve_profile(&self.inner.data_root, name) {
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
        if held_accounts
            .get(&harness)
            .is_some_and(|held| held != &account)
        {
            return Ok(Err(RouteSkipReason::Unauthorized));
        }
        if !native_installed(&harness, &snapshot) {
            return Ok(Err(RouteSkipReason::NotInstalled));
        }
        let unavailable = self
            .inner
            .store
            .lock()
            .await
            .routing_unavailable(name, &harness, &snapshot)?;
        if let Some(reason) = unavailable {
            if reason == RouteSkipReason::ProviderHold {
                held_accounts.insert(harness, account);
            }
            return Ok(Err(reason));
        }
        if matches!(harness.as_str(), "codex" | "muse") {
            if let Ok(raw) = self
                .quota_for_snapshot(harness.clone(), Some(snapshot.clone()), name.to_owned())
                .await
            {
                *quota_status = match raw.get("status").and_then(Value::as_str) {
                    Some("supported") => RouteQuotaStatus::Supported,
                    Some("unsupported") => RouteQuotaStatus::Unsupported,
                    _ => RouteQuotaStatus::Unknown,
                };
                if *quota_status == RouteQuotaStatus::Supported
                    && quota_exhausted(
                        &harness,
                        raw.get("details").unwrap_or(&Value::Null),
                        snapshot.get("model").and_then(Value::as_str),
                    )
                {
                    held_accounts.insert(harness, account);
                    return Ok(Err(RouteSkipReason::QuotaExhausted));
                }
            }
        } else {
            *quota_status = RouteQuotaStatus::Unsupported;
        }
        // Passive reads can take time. Recheck live state before freezing.
        if let Some(reason) = self
            .inner
            .store
            .lock()
            .await
            .routing_unavailable(name, &harness, &snapshot)?
        {
            if reason == RouteSkipReason::ProviderHold {
                held_accounts.insert(harness, account);
            }
            return Ok(Err(reason));
        }
        Ok(Ok((harness, snapshot)))
    }
}

fn account_identity(snapshot: &Value) -> Value {
    json!({"account":snapshot.get("account"), "native_store":snapshot.get("native_store_binding"),
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

/// Only the installed, projected native schemas establish exhaustion. No
/// generic numeric search or model inference probe is used.
fn quota_exhausted(harness: &str, details: &Value, model: Option<&str>) -> bool {
    let full = |window: &Value| {
        window
            .get("usedPercent")
            .and_then(Value::as_f64)
            .is_some_and(|used| used >= 100.0)
            && window
                .get("resetsAtMs")
                .and_then(Value::as_i64)
                .or_else(|| {
                    window
                        .get("resetsAt")
                        .and_then(Value::as_i64)
                        .map(|seconds| seconds.saturating_mul(1000))
                })
                .is_none_or(|reset| reset > now_ms())
    };
    if harness == "muse" {
        let usage = details.get("usage").unwrap_or(details);
        return ["window", "weekly"].iter().any(|key| full(&usage[key]));
    }
    if harness == "codex" {
        let exhausted = |rates: &Value| {
            let credits = &rates["credits"];
            let credit_available = credits["unlimited"].as_bool() == Some(true)
                || credits["hasCredits"].as_bool() == Some(true);
            !credit_available && ["primary", "secondary"].iter().any(|key| full(&rates[key]))
        };
        if exhausted(&details["rateLimits"]) {
            return true;
        }
        // Per-limit windows are relevant only when their id is the exact
        // requested model. An unrelated exhausted model never blocks this one.
        return model
            .and_then(|model| details["rateLimitsByLimitId"].get(model))
            .is_some_and(exhausted);
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn role_selects_first_free_candidate_and_retains_decision_on_agent() {
        let root = std::env::temp_dir().join(format!("eliot-routing-{}", ulid::Ulid::new()));
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
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn preview_reports_the_choice_and_misses_without_admitting() {
        let root = std::env::temp_dir().join(format!("eliot-routing-{}", ulid::Ulid::new()));
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
                .is_empty()
        );
        drop(router);
        std::fs::remove_dir_all(root).unwrap();
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
        assert!(!quota_exhausted(
            "codex",
            &json!({"rateLimits":{"primary":{"usedPercent":100,"resetsAt":1}}}),
            None
        ));
    }
    #[test]
    fn quota_checks_only_known_native_windows_and_preserves_unknown() {
        assert!(quota_exhausted(
            "codex",
            &json!({"rateLimits":{"primary":{"usedPercent":100}}}),
            None
        ));
        assert!(!quota_exhausted(
            "codex",
            &json!({"rateLimits":{"primary":{"usedPercent":100},"credits":{"hasCredits":true}}}),
            None
        ));
        assert!(quota_exhausted(
            "muse",
            &json!({"usage":{"weekly":{"usedPercent":100}}}),
            None
        ));
        assert!(!quota_exhausted("muse", &json!({}), None));
        assert!(!quota_exhausted(
            "codex",
            &json!({"rateLimitsByLimitId":{"other":{"primary":{"usedPercent":100}}}}),
            Some("mine")
        ));
    }
}
