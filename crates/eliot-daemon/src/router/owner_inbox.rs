use super::{Principal, Router, SubmissionContext, require_owner};
use eliot_core::{
    Error, Result,
    api::{
        OwnerDecideRequest, OwnerDecideResponse, OwnerInboxItem, OwnerInboxItemRef,
        OwnerInboxRequest, OwnerInboxResponse, SwarmLiveness, discovery::decode_params,
    },
};
use eliot_plugin::sync::LockExt;
use eliot_store::{OwnerDecisionAdmission, Store, StuckStates};
use serde_json::{Value, json};
use std::sync::Arc;

struct DecisionBinding {
    item: OwnerInboxItemRef,
    action_id: String,
}

tokio::task_local! {
    static OWNER_DECISION: DecisionBinding;
}

/// An inbox choice adds this check to the ordinary mutation while it holds the
/// Store writer. Direct calls continue to use their normal admission rules.
pub(crate) fn check_binding(store: &Store) -> Result<()> {
    OWNER_DECISION
        .try_with(|binding| {
            store.check_owner_decision(&binding.item, &binding.action_id, super::now_ms())
        })
        .unwrap_or(Ok(()))
}

impl Router {
    pub(super) fn confirmed_stuck(&self) -> Arc<StuckStates> {
        self.inner.confirmed_stuck.lock_recover().clone()
    }

    pub(super) async fn read_owner_projection(
        &self,
    ) -> Result<(Vec<OwnerInboxItem>, SwarmLiveness)> {
        let path = self.inner.data_root.join("eliot.db");
        let confirmed = self.confirmed_stuck();
        tokio::task::spawn_blocking(move || {
            Store::read_owner_projection(&path, super::now_ms(), &confirmed)
        })
        .await
        .map_err(|_| Error::new("internal", "owner projection reader failed"))?
    }

    pub(super) async fn owner_inbox(&self, params: &Value, principal: &Principal) -> Result<Value> {
        require_owner(principal)?;
        let request: OwnerInboxRequest = decode_params(params.clone())?;
        let limit = request.effective_limit()?;
        let (items, _) = self.read_owner_projection().await?;
        let mut items = items
            .into_iter()
            .map(|item| (item.cursor(), item))
            .collect::<Vec<_>>();
        items.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        let mut page = items
            .into_iter()
            .filter(|(cursor, _)| request.after.as_ref().is_none_or(|after| cursor > after))
            .take(limit + 1)
            .collect::<Vec<_>>();
        let next = if page.len() > limit {
            page.pop();
            page.last().map(|(cursor, _)| cursor.clone())
        } else {
            None
        };
        serde_json::to_value(OwnerInboxResponse {
            items: page.into_iter().map(|(_, item)| item).collect(),
            next,
        })
        .map_err(|_| Error::new("internal", "owner inbox could not be encoded"))
    }

    pub(super) async fn owner_decide(
        &self,
        params: &Value,
        principal: &Principal,
        context: SubmissionContext<'_>,
    ) -> Result<Value> {
        require_owner(principal)?;
        if super::contains_known_secret_including_keys(params, &self.inner.secrets) {
            return Err(Error::new(
                "secret_in_payload",
                "owner decision contains a known credential",
            ));
        }
        let request: OwnerDecideRequest = decode_params(params.clone())?;
        request.validate()?;
        let admission = self.inner.store.lock().await.admit_owner_decision(
            &request,
            ulid::Ulid::new().to_string(),
            super::now_ms(),
            &self.confirmed_stuck(),
        )?;
        let OwnerDecisionAdmission::Action { id, mut action } = admission else {
            return Ok(json!(OwnerDecideResponse {
                snoozed: true,
                result: None
            }));
        };
        let read_only = matches!(
            action.method.as_str(),
            "agent.get"
                | "command.get"
                | "goal.get"
                | "launch.intent.get"
                | "launch.queue"
                | "timer.get"
                | "provider.holds"
                | "diagnose"
        );
        if !read_only {
            action.args["id"] = json!(id);
        }
        let result = OWNER_DECISION
            .scope(
                DecisionBinding {
                    item: request.item,
                    action_id: id,
                },
                async {
                    if read_only {
                        {
                            let store = self.inner.store.lock().await;
                            check_binding(&store)?;
                        }
                        // Inspection choices remain useful when no correction
                        // has a safe existing mutation.
                        return self
                            .owner_inbox_inspect(action.method.as_str(), &action.args, principal)
                            .await;
                    }
                    match action.method.as_str() {
                        "command.submit"
                            if matches!(
                                action.args["verb"].as_str(),
                                Some("approve" | "deny" | "answer" | "resume" | "stop")
                            ) =>
                        {
                            let result = self.submit_bound(&action.args, principal, context).await;
                            self.note_command_submission(
                                principal,
                                context.remote,
                                "owner.decide",
                                &result,
                            );
                            result
                        }
                        "command.resolve" => self.command_resolve(&action.args, principal).await,
                        "command.settle_stopped" => {
                            self.command_settle_stopped(&action.args, principal).await
                        }
                        "turn.resolve" => self.turn_resolve(&action.args, principal).await,
                        "provider.clear_hold" => {
                            self.provider_clear_hold(&action.args, principal).await
                        }
                        "goal.resume" => self.goal_resume(&action.args, principal).await,
                        "goal.clear" => self.goal_clear(&action.args, principal).await,
                        "goal.round" => self.goal_round(&action.args, principal).await,
                        _ => Err(Error::new(
                            "internal",
                            "owner inbox produced an unsupported action",
                        )),
                    }
                },
            )
            .await?;
        Ok(json!(OwnerDecideResponse {
            snoozed: false,
            result: Some(result)
        }))
    }

    async fn owner_inbox_inspect(
        &self,
        method: &str,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        match method {
            "agent.get" => self.agent_get(params, principal).await,
            "command.get" => self.command_get(params, principal).await,
            "goal.get" => self.goal_get(params, principal).await,
            "launch.intent.get" => self.read_launch_intent(params, principal).await,
            "launch.queue" => self.read_launch_queue(params, principal).await,
            "timer.get" => self.timer_get(params, principal).await,
            "provider.holds" => self.provider_holds(params, principal).await,
            "diagnose" => self.diagnose(params, principal).await,
            _ => Err(Error::new(
                "internal",
                "owner inbox produced an unsupported read",
            )),
        }
    }

    pub(super) async fn sweep_liveness(&self) -> Result<StuckStates> {
        let path = self.inner.data_root.join("eliot.db");
        let sweep = tokio::task::spawn_blocking(move || {
            Store::read_owner_inbox_sweep(&path, super::now_ms())
        })
        .await
        .map_err(|_| Error::new("internal", "owner inbox sweep reader failed"))??;
        if !sweep.retired_snoozes.is_empty() {
            self.inner
                .store
                .lock()
                .await
                .prune_owner_inbox_snoozes(&sweep.retired_snoozes, super::now_ms())?;
        }
        Ok(sweep.candidates)
    }

    pub(super) fn publish_liveness(&self, current: StuckStates, previous: &mut StuckStates) {
        let old = self.confirmed_stuck();
        let confirmed: StuckStates = current
            .iter()
            .filter(|(subject, state)| {
                previous.get(*subject) == Some(*state) || old.get(*subject) == Some(*state)
            })
            .map(|(subject, state)| (subject.clone(), state.clone()))
            .collect();
        let policy = self.secret_policy_snapshot();
        for (subject, state) in &confirmed {
            if old.get(subject) != Some(state) {
                crate::logging::record_liveness_stuck(&policy, subject, &state.reason);
            }
        }
        *self.inner.confirmed_stuck.lock_recover() = Arc::new(confirmed);
        *previous = current;
    }
}
