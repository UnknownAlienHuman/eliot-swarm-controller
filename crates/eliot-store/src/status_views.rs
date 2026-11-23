//! Batched, generation-bound status projections shared by fleet and direct reads.

use super::*;
use std::collections::BTreeSet;

pub(super) struct AgentStatusLookup {
    pub(super) agent_num: i64,
    pub(super) generation: i64,
    pub(super) active_turn: Option<String>,
    pub(super) last_progress_at: Option<i64>,
    pub(super) provider_scope: String,
}

pub(super) struct AgentStatusProjection {
    pub(super) provider_hold: Option<core::api::AgentProviderHold>,
    pub(super) native_notice: Option<core::api::AgentNativeNotice>,
}

impl Store {
    /// Read hold and notice facts through the same projection used by fleet pages.
    pub fn agent_status_projection(
        &self,
        agent: &Agent,
    ) -> Result<(
        Option<core::api::AgentProviderHold>,
        Option<core::api::AgentNativeNotice>,
    )> {
        let agent_num = parse_agent_id(&agent.id)?;
        let last_progress_at =
            native_children::native_progress(&self.connection, agent_num, agent.generation)?;
        let lookup = AgentStatusLookup {
            agent_num,
            generation: agent.generation,
            active_turn: agent.active_turn.clone(),
            last_progress_at,
            provider_scope: core::frozen_provider_scope_for(
                &agent.profile,
                &agent.harness,
                &agent.profile_snapshot,
            )?,
        };
        let mut projections = self.project_agent_statuses(std::slice::from_ref(&lookup))?;
        let projection = projections
            .pop()
            .ok_or_else(|| Error::new("internal", "Agent status projection is missing"))?;
        Ok((projection.provider_hold, projection.native_notice))
    }

    /// Resolve both status surfaces with one bounded hold query and one
    /// page-scoped event query. Callers retain input order for their rows.
    pub(super) fn project_agent_statuses(
        &self,
        lookups: &[AgentStatusLookup],
    ) -> Result<Vec<AgentStatusProjection>> {
        if lookups.is_empty() {
            return Ok(Vec::new());
        }

        let scopes: Vec<String> = lookups
            .iter()
            .map(|lookup| lookup.provider_scope.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let holds = self.agent_provider_holds(&scopes)?;
        let notices = native_events::agent_native_notices(&self.connection, lookups)?;
        if notices.len() != lookups.len() {
            return Err(Error::new(
                "internal",
                "Agent native notice page is incomplete",
            ));
        }

        Ok(lookups
            .iter()
            .zip(notices)
            .map(|(lookup, native_notice)| {
                let mut provider_hold = holds.get(&lookup.provider_scope).cloned();
                if let Some(hold) = &mut provider_hold {
                    hold.waiting_for_active_turn = lookup.active_turn.is_some()
                        && hold.state == core::ProviderHoldState::Probing
                        && !lookup
                            .last_progress_at
                            .is_some_and(|progress| progress > hold.updated_at);
                }
                let native_notice = native_notice
                    .filter(|read| {
                        lookup.active_turn.is_some()
                            || !read.provider_failure
                            || provider_hold
                                .as_ref()
                                .is_some_and(|hold| read.notice.provider_kind == Some(hold.kind))
                    })
                    .map(|read| read.notice);
                AgentStatusProjection {
                    provider_hold,
                    native_notice,
                }
            })
            .collect())
    }
}
