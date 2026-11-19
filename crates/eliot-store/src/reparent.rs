//! Atomic Owner hierarchy edits. Native identity, custody and snapshots stay put.
use super::*;
use eliot_core::api::{ReparentRequest, ReparentResponse, ReparentedAgent};
use std::collections::HashSet;

impl Store {
    pub fn reparent(
        &mut self,
        principal: &str,
        request: &ReparentRequest,
        now: i64,
    ) -> Result<Change<ReparentResponse>> {
        self.ensure_writer_healthy()?;
        let result = self.reparent_inner(principal, request, now);
        self.fault_on_persistence_error(&result);
        result
    }

    fn reparent_inner(
        &mut self,
        principal: &str,
        request: &ReparentRequest,
        now: i64,
    ) -> Result<Change<ReparentResponse>> {
        request.validate()?;
        if principal != "owner" {
            return Err(Error::new("not_allowed", "only Owner may change a manager"));
        }
        let digest = canonical::digest(
            &serde_json::to_value(request)
                .map_err(|_| Error::new("internal", "cannot encode reparent request"))?,
        )?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        // Retained admission precedes generation, topology and fanout checks.
        if command_binding(&tx, &request.id)?.is_some() || digest_id_bound(&tx, &request.id)? {
            return Err(Error::new(
                "id_conflict",
                "mutation id is used by a command",
            ));
        }
        if let Some((old_principal, method, old_digest, result)) = write_binding(&tx, &request.id)?
        {
            if old_principal != principal || method != "swarm.reparent" || old_digest != digest {
                return Err(Error::new("id_conflict", "mutation id has another binding"));
            }
            let value = serde_json::from_value(decode_json(&result)?)
                .map_err(|_| Error::new("source_integrity", "invalid retained reparent result"))?;
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(Change {
                value,
                events: Vec::new(),
            });
        }
        validate_id_age(&request.id, now)?;
        for id in [request.source()?, &request.parent] {
            if let Some(agent) = load_agent_by_num(&tx, parse_agent_id(id)?)? {
                project_transfer::require_no_intent(&tx, &agent.project)?;
            }
        }
        let Change { value, events } =
            reparent_tx(&tx, request, self.hierarchy_limits, now, false)?;
        let result = serde_json::to_value(&value)
            .map_err(|_| Error::new("internal", "cannot encode reparent result"))?;
        coordination::retain_write_tx(
            &tx,
            &request.id,
            principal,
            "swarm.reparent",
            &digest,
            &result,
            now,
        )?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok(Change { value, events })
    }
}

/// Shared link edit inside the caller's transaction. No separate write or commit.
pub(super) fn reparent_tx(
    tx: &Transaction<'_>,
    request: &ReparentRequest,
    limits: eliot_core::api::HierarchyLimits,
    now: i64,
    controlled_only: bool,
) -> Result<Change<ReparentResponse>> {
    let source = load_agent_by_num(tx, parse_agent_id(request.source()?)?)?
        .ok_or_else(|| Error::new("not_found", "source Agent was not found"))?;
    let parent_num = parse_agent_id(&request.parent)?;
    let parent = load_agent_by_num(tx, parent_num)?
        .ok_or_else(|| Error::new("not_found", "new parent was not found"))?;
    if source.generation != request.expected_generation
        || parent.generation != request.parent_generation
    {
        return Err(Error::new("stale_generation", "reparent generation moved"));
    }
    observer::require_controlled(&parent)?;
    if parent.is_terminal() {
        return Err(Error::new("bad_state", "new parent is terminal"));
    }
    if source.observe_only {
        return Err(Error::new(
            "not_allowed",
            "native child parentage is harness-owned",
        ));
    }
    let cap = limits.max_selector_targets;
    let roots = if request.children_of.is_some() {
        let mut stmt = tx
            .prepare(
                "SELECT id FROM agents WHERE parent = ?1 AND (NOT ?3 OR observe_only = 0)
                 ORDER BY id LIMIT ?2",
            )
            .map_err(sql_error)?;
        stmt.query_map(
            params![
                parse_agent_id(&source.id)?,
                cap.saturating_add(1),
                controlled_only
            ],
            |row| row.get::<_, i64>(0),
        )
        .map_err(sql_error)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(sql_error)?
    } else {
        vec![parse_agent_id(&source.id)?]
    };
    let mut seen = HashSet::new();
    let mut moves = Vec::new();
    for root in roots {
        let root_agent = load_agent_by_num(tx, root)?
            .ok_or_else(|| Error::new("source_integrity", "moving root disappeared"))?;
        let members = if controlled_only {
            hierarchy::controlled_subtree_member_nums(tx, root, cap)?
        } else {
            hierarchy::subtree_member_nums(tx, root, cap)?
        };
        for num in members {
            if !seen.insert(num) || moves.len() as i64 >= cap {
                return Err(Error::new(
                    "fanout_limit",
                    "reparent expansion exceeds limit",
                ));
            }
            if num == parent_num {
                return Err(Error::new("invalid", "reparent would create a cycle"));
            }
            let agent = load_agent_by_num(tx, num)?
                .ok_or_else(|| Error::new("source_integrity", "moving member disappeared"))?;
            if agent.observe_only {
                return Err(Error::new(
                    "not_allowed",
                    "native child parentage is harness-owned",
                ));
            }
            if agent.project_scope() != parent.project_scope() {
                return Err(Error::new(
                    "not_allowed",
                    "reparent must stay in the same project",
                ));
            }
            let relative_depth = agent
                .depth
                .checked_sub(root_agent.depth)
                .ok_or_else(|| Error::new("fanout_limit", "reparent depth overflow"))?;
            let depth = parent
                .depth
                .checked_add(1)
                .and_then(|depth| depth.checked_add(relative_depth))
                .ok_or_else(|| Error::new("fanout_limit", "reparent depth overflow"))?;
            if depth > limits.max_depth || depth < 1 {
                return Err(Error::new("fanout_limit", "reparent depth exceeds limit"));
            }
            moves.push((
                num,
                ReparentedAgent {
                    agent: agent.id,
                    generation: agent.generation,
                    previous_parent: agent.parent.clone(),
                    parent: if num == root {
                        Some(parent.id.clone())
                    } else {
                        agent.parent
                    },
                    root: parent.root.clone(),
                    depth,
                },
            ));
        }
    }
    // Check the final topology in this transaction; every error rolls it back.
    for (num, moved) in &moves {
        tx.execute(
            "UPDATE agents SET parent = ?2, root = ?3, depth = ?4 WHERE id = ?1",
            params![
                num,
                moved.parent.as_deref().map(parse_agent_id).transpose()?,
                parse_agent_id(&moved.root)?,
                moved.depth
            ],
        )
        .map_err(sql_error)?;
    }
    if hierarchy::count_children_tx(tx, parent_num)? > limits.max_children_per_agent
        || hierarchy::count_tree_tx(tx, parse_agent_id(&parent.root)?)? > limits.max_tree_agents
    {
        return Err(Error::new(
            "fanout_limit",
            "reparent exceeds destination fanout",
        ));
    }
    moves.sort_by_key(|(num, _)| *num);
    let value = ReparentResponse {
        id: request.id.clone(),
        moved: moves.into_iter().map(|(_, row)| row).collect(),
    };
    let mut events = Vec::new();
    for moved in &value.moved {
        events.push(insert_daemon_event(tx, now, "agent.reparented", "coordination",
            Some(parse_agent_id(&moved.agent)?), Some(moved.generation),
            json!({"id":request.id,"previous_parent":moved.previous_parent,"parent":moved.parent,"root":moved.root,"depth":moved.depth}))?);
    }
    Ok(Change { value, events })
}
