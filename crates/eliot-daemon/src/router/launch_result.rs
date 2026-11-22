use super::Router;
use crate::git_monitor::GitProbe;
use eliot_core::{
    Command, Error, Event, Result,
    api::{MailDefinitionArgs, ProjectManagerIdentity},
    fence_untrusted_text,
};

pub(crate) const RULE_WORKTREE_GUARD_ARG: &str = "_rule_worktree_guard";

fn event_worktree_fingerprint(event: &Event) -> Option<&str> {
    event.data.pointer("/worktree/fingerprint")?.as_str()
}

impl Router {
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

    pub(crate) async fn rule_launch_result_mail(
        &self,
        principal: &str,
        rule: &str,
        to: Option<&str>,
        terminal: &Event,
    ) -> Result<Option<MailDefinitionArgs>> {
        let Some(origin) = self
            .inner
            .store
            .lock()
            .await
            .rule_launch_trigger(principal, rule, terminal)?
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
        let recipient = if let Some(to) = to {
            to.to_owned()
        } else {
            let store = self.inner.store.lock().await;
            let author = store
                .get_agent(author)?
                .ok_or_else(|| Error::new("not_found", "review author is no longer retained"))?;
            if let Some(parent) = author.parent {
                parent
            } else {
                match store.project_assignment("owner", &author.project)?.manager {
                    Some(ProjectManagerIdentity::Local { agent, generation })
                        if store
                            .get_agent(&agent)?
                            .is_some_and(|current| current.generation == generation) =>
                    {
                        agent
                    }
                    _ => {
                        return Err(Error::new(
                            "invalid",
                            "author has no local manager; configure a mail recipient",
                        ));
                    }
                }
            }
        };
        let answer = terminal.data["final_text"]
            .as_str()
            .filter(|text| !text.trim().is_empty())
            .ok_or_else(|| Error::new("invalid", "launch result has no retained final text"))?;
        let state = if self.rule_worktree_is_current(principal, &origin).await? {
            "Fingerprint still current at delivery check."
        } else {
            "Fingerprint changed or could not be confirmed at delivery check."
        };
        Ok(Some(MailDefinitionArgs {
            to: vec![recipient],
            thread: Some("review".to_owned()),
            reply_to_delivery: None,
            text: format!(
                "Worktree review for Agent {author}, turn {turn}\nFingerprint {fingerprint}\n{state}\n{}",
                fence_untrusted_text(answer),
            ),
        }))
    }
}
