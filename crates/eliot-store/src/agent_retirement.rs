//! Archive quiet, unbound Agents without changing their generation or history.
use super::*;
use context::{UNDISPATCHED_SWARM_NOTE_SQL, withdraw_swarm_notes_tx};

const PAGE: i64 = 64;

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AgentRetirementPolicy {
    pub enabled: bool,
    pub ended_after_seconds: i64,
    pub observed_idle_after_seconds: i64,
    pub pinned_agents: Vec<String>,
}

impl Default for AgentRetirementPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            ended_after_seconds: 86_400,
            observed_idle_after_seconds: 604_800,
            pinned_agents: Vec::new(),
        }
    }
}

impl AgentRetirementPolicy {
    pub fn validate(&self) -> Result<()> {
        if [self.ended_after_seconds, self.observed_idle_after_seconds]
            .into_iter()
            .any(|seconds| !(1..=i64::MAX / 1_000).contains(&seconds))
        {
            return Err(Error::new(
                "invalid",
                "Agent retirement thresholds must fit positive milliseconds",
            ));
        }
        for agent in &self.pinned_agents {
            parse_agent_id(agent)?;
        }
        Ok(())
    }
}

// A managed checkout binds the session until guarded cleanup resolves it.
// Missing-path reconciliation and retained branches do not establish that
// unpublished work is gone or integrated.
const WORKTREES_RESOLVED: &str = "
    NOT EXISTS (
        SELECT 1 FROM workspaces w WHERE w.agent=a.id
          AND NOT (w.phase='removed' AND (
              EXISTS (SELECT 1 FROM child_cleanup cc WHERE cc.child=w.agent
                       AND cc.child_generation=w.target_generation AND cc.state='removed')
              OR (json_extract(w.outcome,'$.result.state') IS 'removed'
                  AND json_extract(w.outcome,'$.result.branch_retained') IS 0
                  AND json_extract(w.outcome,'$.result.directory_absent_before_remove') IS NOT 1
                  AND json_extract(w.outcome,'$.result.branch_absent_before_remove') IS NOT 1)
          ))
    )
    AND NOT EXISTS (
        SELECT 1 FROM artifacts ar WHERE ar.agent=a.id
          AND json_extract(ar.body,'$.kind')='worktree'
          AND json_extract(ar.body,'$.state') IS NOT 'removed'
    )";

// The newest result and receipt both use the exact generation and current
// parent. Reading a mailbox summary does not prove result readback.
const LATEST_COMPLETED_CHILD_RESULT: &str = "
    SELECT t.turn_id FROM turns t WHERE t.agent=a.id AND t.generation=a.generation
      AND t.insertion_seq=(SELECT MAX(insertion_seq) FROM turns
                           WHERE agent=a.id AND generation=a.generation)
      AND t.state='completed'";
const RESULT_READ: &str = "
    EXISTS (SELECT 1 FROM agent_result_reads r WHERE r.child=a.id
            AND r.generation=a.generation AND r.parent=a.parent
            AND r.turn_id=latest.turn_id)";

impl Store {
    /// Record the exact current result returned to the authenticated direct
    /// parent. Historical or partial result pages cannot mark newer work read.
    pub fn record_child_result_read(
        &mut self,
        parent: &str,
        parent_generation: i64,
        child: &str,
        generation: i64,
        turn_id: &str,
    ) -> Result<()> {
        self.ensure_writer_healthy()?;
        let result = self
            .connection
            .execute(
                "INSERT INTO agent_result_reads(child,generation,parent,turn_id)
             SELECT a.id,a.generation,p.id,t.turn_id FROM agents a
             JOIN agents p ON p.id=a.parent
             JOIN turns t ON t.agent=a.id AND t.generation=a.generation
             WHERE p.id=?1 AND p.generation=?2 AND a.id=?3 AND a.generation=?4
               AND t.turn_id=?5 AND t.state='completed'
               AND t.insertion_seq=(SELECT MAX(insertion_seq) FROM turns
                                    WHERE agent=a.id AND generation=a.generation)
             ON CONFLICT(child,generation) DO UPDATE SET
                parent=excluded.parent, turn_id=excluded.turn_id
             WHERE parent<>excluded.parent OR turn_id<>excluded.turn_id",
                params![
                    parse_agent_id(parent)?,
                    parent_generation,
                    parse_agent_id(child)?,
                    generation,
                    turn_id,
                ],
            )
            .map_err(sql_error);
        self.fault_on_persistence_error(&result);
        result.map(|_| ())
    }

    /// Apply one bounded page. The checks and archive share the Store write
    /// transaction. The resulting rows remain available to all history reads.
    pub fn archive_retired_agents(
        &mut self,
        policy: &AgentRetirementPolicy,
        after: Option<i64>,
        now: i64,
    ) -> Result<Change<Option<i64>>> {
        if !policy.enabled {
            return Ok(Change {
                value: None,
                events: Vec::new(),
            });
        }
        policy.validate()?;
        self.ensure_writer_healthy()?;
        let result = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let custody = super::agent_lifecycle::default_list_custody_agents(&tx)?;
            let pinned = policy
                .pinned_agents
                .iter()
                .map(|id| parse_agent_id(id))
                .collect::<Result<Vec<_>>>()?;
            let excluded = custody.into_iter().chain(pinned).collect::<Vec<_>>();
            let sql = format!(
                "WITH page AS MATERIALIZED (
                   SELECT id FROM agents WHERE id>?1 AND archived=0 ORDER BY id LIMIT ?2
                 )
                 SELECT a.id,a.generation,CASE
                    WHEN a.observe_only=1 THEN 'observed_idle'
                    WHEN successor.successor IS NOT NULL THEN 'handed_off'
                    ELSE 'child_result_read' END AS reason
                 FROM page JOIN agents a USING(id)
                 LEFT JOIN turns latest ON latest.agent=a.id AND latest.generation=a.generation
                    AND latest.turn_id=({LATEST_COMPLETED_CHILD_RESULT})
                 LEFT JOIN agent_successors successor ON successor.agent=a.id AND successor.generation=a.generation
                    AND EXISTS (SELECT 1 FROM agents next WHERE next.id=successor.successor)
                 WHERE a.id NOT IN (SELECT value FROM json_each(?5))
                   AND a.capacity_generation IS NULL AND a.active_turn IS NULL AND a.pending='[]'
                   AND ((a.observe_only=1 AND a.state IN ('idle','stopped') AND a.updated_at<=?4)
                     OR (a.observe_only=0 AND a.updated_at<=?3 AND (
                       (successor.successor IS NOT NULL AND a.state IN ('stopped','failed','lost')
                          AND (a.parent IS NULL OR {RESULT_READ}))
                       OR (a.parent IS NOT NULL AND a.state IN ('idle','stopped')
                           AND a.origin='eliot' AND {RESULT_READ}))))
                   AND NOT EXISTS (SELECT 1 FROM commands c WHERE c.target=a.id
                                    AND c.state IN ('accepted','queued','dispatched','unknown')
                                    AND NOT {UNDISPATCHED_SWARM_NOTE_SQL})
                   AND NOT EXISTS (SELECT 1 FROM turns t WHERE t.agent=a.id AND t.state IN ('running','unknown'))
                   AND NOT EXISTS (SELECT 1 FROM pending_requests p WHERE p.agent=a.id AND p.state='pending')
                   AND NOT EXISTS (SELECT 1 FROM goals g WHERE g.agent=a.id
                                    AND json_extract(g.state,'$.status') IN ('active','paused','blocked'))
                   AND NOT EXISTS (SELECT 1 FROM goal_markers g WHERE g.agent=a.id)
                   AND NOT EXISTS (SELECT 1 FROM project_assignments p WHERE p.manager=a.id)
                   AND NOT EXISTS (
                       SELECT 1 FROM completion_digests digest
                       LEFT JOIN messages mail ON mail.delivery_id=COALESCE(digest.transferred_mail_id,digest.delivered_mail_id)
                          AND mail.recipient=a.id
                       WHERE digest.parent=a.id AND mail.read_at IS NULL
                   )
                   AND NOT EXISTS (
                       WITH RECURSIVE descendants(id,state,capacity_generation) AS (
                           SELECT id,state,capacity_generation FROM agents WHERE parent=a.id
                           UNION ALL
                           SELECT c.id,c.state,c.capacity_generation FROM agents c
                           JOIN descendants d ON c.parent=d.id
                       )
                       SELECT 1 FROM descendants d
                       WHERE d.state IN ('created','starting','idle','busy','waiting','unknown')
                          OR d.capacity_generation IS NOT NULL
                          OR d.id IN (SELECT value FROM json_each(?5))
                          OR EXISTS (
                              SELECT 1 FROM agents a JOIN turns latest
                                ON latest.agent=a.id AND latest.generation=a.generation
                               AND latest.turn_id=({LATEST_COMPLETED_CHILD_RESULT})
                              WHERE a.id=d.id AND NOT {RESULT_READ}
                          )
                   )
                   AND {WORKTREES_RESOLVED}
                 ORDER BY a.id"
            );
            let page_end: (i64, Option<i64>) = tx.query_row(
                "SELECT COUNT(*),MAX(id) FROM (SELECT id FROM agents WHERE id>?1 AND archived=0 ORDER BY id LIMIT ?2)",
                params![after.unwrap_or(0), PAGE], |r| Ok((r.get(0)?,r.get(1)?)),
            ).map_err(sql_error)?;
            let rows = {
                let mut statement = tx.prepare_cached(&sql).map_err(sql_error)?;
                statement
                    .query_map(
                        params![
                            after.unwrap_or(0),
                            PAGE,
                            now.saturating_sub(policy.ended_after_seconds * 1_000),
                            now.saturating_sub(policy.observed_idle_after_seconds * 1_000),
                            encode_json(&excluded)?
                        ],
                        |r| {
                            Ok((
                                r.get::<_, i64>(0)?,
                                r.get::<_, i64>(1)?,
                                r.get::<_, String>(2)?,
                            ))
                        },
                    )
                    .map_err(sql_error)?
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .map_err(sql_error)?
            };
            let mut events = Vec::with_capacity(rows.len());
            for (id, generation, reason) in rows {
                withdraw_swarm_notes_tx(&tx, id, "agent_archived", now, Some(&mut events))?;
                db!(tx.execute("UPDATE agents SET archived=1 WHERE id=?1", [id]));
                events.push(insert_daemon_event(
                    &tx,
                    now,
                    "agent.archived",
                    "agent",
                    Some(id),
                    Some(generation),
                    json!({"reason":reason,"automatic":true}),
                )?);
            }
            commit_transaction(tx, &self.write_faulted)?;
            Ok(Change {
                value: if page_end.0 == PAGE { page_end.1 } else { None },
                events,
            })
        })();
        self.fault_on_persistence_error(&result);
        result
    }
}
