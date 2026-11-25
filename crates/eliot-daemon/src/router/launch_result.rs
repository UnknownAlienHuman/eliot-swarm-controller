use super::{PreparedSubmission, Router};
use crate::git_monitor::GitProbe;
use eliot_core::{
    Command, Error, Event, InjectArgs, InjectPoint, Result, ReviewWorktreeState, SubmitCommand,
    TrustedReviewLabels,
    api::{ProjectRegistrationView, catalog_provider},
};
use serde_json::Value;

pub(crate) const RULE_WORKTREE_GUARD_ARG: &str = "_rule_worktree_guard";

fn event_worktree_fingerprint(event: &Event) -> Option<&str> {
    event.data.pointer("/worktree/fingerprint")?.as_str()
}

struct ReviewModel {
    provider: String,
    family: String,
}

impl ReviewModel {
    fn new(harness: &str, model: &str) -> Option<Self> {
        let family = eliot_core::api::model_family(model)?;
        Some(Self {
            provider: catalog_provider(harness, model).to_ascii_lowercase(),
            family,
        })
    }

    fn independent(&self, snapshot: &Value) -> bool {
        snapshot["harness"]
            .as_str()
            .zip(snapshot["model"].as_str())
            .and_then(|(harness, model)| Self::new(harness, model))
            .is_some_and(|model| model.provider != self.provider && model.family != self.family)
    }
}

impl Router {
    pub(crate) async fn prepare_independent_review(
        &self,
        mut action: SubmitCommand,
        project: Option<&ProjectRegistrationView>,
        event: &Event,
    ) -> Result<PreparedSubmission> {
        let Some(subject) = event.subject.as_ref() else {
            return Err(Error::new("bad_state", "review has no exact author turn"));
        };
        let author_model = self.inner.store.lock().await.review_author_model(event)?;
        let Some(author_model) = author_model else {
            return Err(Error::new(
                "bad_state",
                "review author model is not retained",
            ));
        };
        let Some(author) = ReviewModel::new(&subject.harness, &author_model) else {
            return Err(Error::new(
                "invalid",
                format!("unknown model family for review author model {author_model:?}"),
            ));
        };
        let pinned = action.args["profile"].as_str().map(str::to_owned);
        let name = crate::config::select_review_profile(
            &self.inner.data_root,
            pinned.as_deref(),
            action.args["role"].as_str(),
            project.map(|project| project.profiles.as_slice()),
            |name, snapshot| {
                name != subject.profile
                    && snapshot["setup"].as_array().is_none_or(Vec::is_empty)
                    && author.independent(snapshot)
            },
        )?
        .ok_or_else(|| {
            if let Some(pinned) = pinned.as_deref() {
                return Error::new(
                    "routing_unavailable",
                    format!(
                        "reviewer profile {pinned:?} cannot review this turn (author {} {}): \
                         the reviewer must be an approved profile with a concrete known model, \
                         no setup entries, and a different provider and model family",
                        author.provider, author.family,
                    ),
                );
            }
            Error::new(
                "routing_unavailable",
                "review requires an approved profile from a different provider and model family",
            )
        })?;
        let args = action.args.as_object_mut().expect("rendered launch args");
        args.remove("role");
        args.insert("profile".to_owned(), Value::String(name.clone()));
        let prepared = match project {
            Some(project) => {
                self.prepare_project_rule_submission(action, project)
                    .await?
            }
            None => self.prepare_owner_submission(action).await?,
        };
        // Settings may change during launch preparation. Check what the
        // ordinary admission path actually froze, including the model.
        if !author.independent(&prepared.request.args["profile_snapshot"]) {
            return Err(Error::new(
                "routing_unavailable",
                format!(
                    "frozen reviewer profile {name:?} no longer has an independent provider and model family",
                ),
            ));
        }
        Ok(prepared)
    }

    pub(super) async fn check_rule_worktree_guard(&self, command: &Command) -> Result<()> {
        if command.verb != "launch" || command.args.get(RULE_WORKTREE_GUARD_ARG).is_none() {
            return Ok(());
        }
        let event = self
            .inner
            .store
            .lock()
            .await
            .rule_action_trigger(&command.id)?;
        if let Some(event) = event
            && self
                .rule_worktree_is_current(&command.principal, &event)
                .await?
        {
            return Ok(());
        }
        Err(Error::new(
            "stale_workspace",
            "rule launch worktree fingerprint could not be verified",
        ))
    }

    pub(crate) async fn rule_worktree_is_current(
        &self,
        principal: &str,
        event: &Event,
    ) -> Result<bool> {
        if event.kind == "turn.completed" && event.data["project_check"] != "off" {
            return Ok(false);
        }
        let workspace = event
            .subject
            .as_ref()
            .and_then(|subject| subject.workspace.as_deref());
        let (Some(agent), Some(generation), Some(workspace), Some(fingerprint), Some(base)) = (
            event.agent.as_deref(),
            event.generation,
            workspace,
            event_worktree_fingerprint(event),
            event
                .data
                .pointer("/worktree/base")
                .and_then(serde_json::Value::as_str),
        ) else {
            return Ok(false);
        };
        let current = self.inner.store.lock().await.get_agent(agent)?;
        if current.is_none_or(|agent| {
            agent.generation != generation
                || agent.workspace.as_deref() != Some(workspace)
                || agent.base_oid.as_deref() != Some(base)
                || principal
                    .strip_prefix("project:")
                    .is_some_and(|project| project != agent.project)
        }) {
            return Ok(false);
        }
        let Ok(git) = GitProbe::new() else {
            return Ok(false);
        };
        Ok(git
            .work_fingerprint(workspace)
            .await
            .is_ok_and(|value| value == fingerprint))
    }

    pub(crate) async fn rule_launch_result_insertion(
        &self,
        principal: &str,
        rule: &str,
        to: Option<&str>,
        terminal: &Event,
    ) -> Result<Option<SubmitCommand>> {
        let Some((origin, recipient)) = self
            .inner
            .store
            .lock()
            .await
            .rule_launch_result(principal, rule, to, terminal)?
        else {
            return Ok(None);
        };
        let fingerprint = event_worktree_fingerprint(&origin)
            .ok_or_else(|| Error::new("invalid", "launch trigger has no worktree fingerprint"))?;
        let author = origin
            .agent
            .as_deref()
            .ok_or_else(|| Error::new("invalid", "launch trigger has no Agent"))?;
        let turn = origin.data["turn_id"]
            .as_str()
            .ok_or_else(|| Error::new("invalid", "launch trigger has no turn ID"))?;
        let answer = terminal.data["final_text"]
            .as_str()
            .filter(|text| !text.trim().is_empty())
            .ok_or_else(|| Error::new("invalid", "launch result has no retained final text"))?;
        let worktree_state = if self.rule_worktree_is_current(principal, &origin).await? {
            ReviewWorktreeState::CurrentAtDeliveryCheck
        } else {
            ReviewWorktreeState::ChangedOrUnconfirmedAtDeliveryCheck
        };
        let mut request = SubmitCommand::new(String::new(), "inject");
        request.target = Some(recipient.id);
        request.expected_generation = Some(recipient.generation);
        request.expected_turn_id = recipient.active_turn;
        request.args = serde_json::to_value(InjectArgs {
            point: InjectPoint::Soonest,
            expires_at_ms: None,
            text: format!("Worktree review for Agent {author}, turn {turn}\n{answer}"),
        })
        .map_err(|_| Error::new("internal", "launch result insertion could not be encoded"))?;
        request.trusted_review_labels = Some(TrustedReviewLabels {
            worktree_fingerprint: fingerprint.to_owned(),
            worktree_state,
        });
        request.validate()?;
        Ok(Some(request))
    }
}
