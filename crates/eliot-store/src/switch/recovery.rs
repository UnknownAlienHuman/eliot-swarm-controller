use super::*;

pub(crate) fn has_releasable_switch_fence(
    connection: &Connection,
    command: &Command,
) -> Result<bool> {
    if command.verb != "switch"
        || !matches!(
            command.state,
            CommandState::Unknown
                | CommandState::Failed
                | CommandState::Rejected
                | CommandState::Cancelled
        )
    {
        return Ok(false);
    }
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM switch_checkout_owners WHERE switch_command = ?1)",
            [&command.id],
            |row| row.get(0),
        )
        .map_err(sql_error)
}

impl Store {
    /// Run after restart custody readback. An interrupted switch never drives a
    /// start or transfer during recovery.
    pub fn reconcile_switch_checkout_fences(&mut self, now: i64) -> Result<Vec<Event>> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let ids = {
                let mut statement = tx
                    .prepare(
                        "SELECT DISTINCT f.switch_command FROM switch_checkout_owners f
                         JOIN commands c ON c.id = f.switch_command
                         WHERE c.method = 'swarm.switch'
                           AND c.state IN ('unknown','failed','rejected','cancelled')
                         ORDER BY f.switch_command",
                    )
                    .map_err(sql_error)?;
                let rows = statement
                    .query_map([], |row| row.get::<_, String>(0))
                    .map_err(sql_error)?;
                rows.collect::<rusqlite::Result<Vec<_>>>()
                    .map_err(sql_error)?
            };
            let mut events = Vec::new();
            for id in ids {
                let parent = switch_parent(&tx, &id)?;
                if let Some(pending) = release_plan(&tx, &parent, false)? {
                    release_tx(&tx, &parent, pending, None, now, &mut events)?;
                }
            }
            commit_transaction(tx, &self.write_faulted)?;
            Ok(events)
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn recoverable_workspace_start(&self, id: &str) -> Result<bool> {
        self.ensure_committed_reads()?;
        self.connection
            .query_row(
                "SELECT NOT EXISTS(SELECT 1 FROM commands child
             LEFT JOIN commands parent ON parent.id = child.parent_command
             WHERE child.id = ?1 AND child.link_kind = 'switch' AND child.ordinal = 0
               AND parent.state IS NOT 'dispatched')",
                [id],
                |row| row.get(0),
            )
            .map_err(sql_error)
    }
}

pub(crate) fn owner_release_switch_fence_tx(
    tx: &Transaction<'_>,
    parent: &Command,
    reason: &str,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<CommandState> {
    let pending = release_plan(tx, parent, true)?.ok_or_else(|| {
        Error::new(
            "busy",
            "stop any live source or successor and settle active workspace operations before releasing the switch fence",
        )
    })?;
    release_tx(tx, parent, pending, Some(reason), now, events)
}

/// Check every participant and pending child before changing any state. The
/// Owner may attest to an unproven exit, but an observed writer still blocks.
fn release_plan(
    tx: &Transaction<'_>,
    parent: &Command,
    owner_assertion: bool,
) -> Result<Option<Vec<Command>>> {
    let plan = stored_result(&parent.outcome)?;
    let retained = decode_switch(parent.args.clone())?;
    let launch = component(tx, &parent.id, &plan.launch_command, 0)?;
    if component(tx, &parent.id, &plan.handoff_command, 1)?.is_some()
        || (launch.is_none() && retained.target_checkout_key.is_some() && !owner_assertion)
        || !checkout_writer_gone(tx, &plan.from, plan.from_generation, owner_assertion)?
    {
        return Ok(None);
    }
    let mut pending = Vec::new();
    if let Some(launch) = launch {
        let (Some(target), Some(generation)) = (launch.target.as_deref(), launch.generation) else {
            return Ok(None);
        };
        if launch.verb != "launch"
            || launch.state == CommandState::Dispatched
            || !checkout_writer_gone(tx, target, generation, owner_assertion)?
        {
            return Ok(None);
        }
        if matches!(launch.state, CommandState::Accepted | CommandState::Queued) {
            if generation != 0 {
                return Ok(None);
            }
            pending.push(launch);
        }
    }
    for (id, ordinal) in [
        (plan.disposal_command.as_deref(), 2),
        (retained.source_stop_command.as_deref(), 3),
    ] {
        if let Some(id) = id
            && let Some(stop) = component(tx, &parent.id, id, ordinal)?
        {
            if stop.verb != "stop" || stop.state == CommandState::Dispatched {
                return Ok(None);
            }
            if matches!(stop.state, CommandState::Accepted | CommandState::Queued) {
                pending.push(stop);
            }
        }
    }
    Ok(Some(pending))
}

fn checkout_writer_gone(
    tx: &Connection,
    agent_id: &str,
    generation: i64,
    owner_assertion: bool,
) -> Result<bool> {
    let num = parse_agent_id(agent_id)?;
    let Some(agent) = load_agent_by_num(tx, num)? else {
        return Ok(owner_assertion);
    };
    if agent.generation != generation
        || !(agent.is_terminal() || generation == 0 && agent.state == AgentState::Created)
        || workspace::load_workspace(tx, num)?.is_some_and(|work| {
            !matches!(
                work.phase,
                WorkspacePhase::Ready
                    | WorkspacePhase::SetupReady
                    | WorkspacePhase::Failed
                    | WorkspacePhase::Removed
            )
        })
    {
        return Ok(false);
    }
    let running: bool = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM processes WHERE state = 'running'
         AND (owner GLOB 'agent:' || ?1 || '#*' OR owner GLOB 'workspace:' || ?1 || '#*'))",
            [agent_id],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if running {
        return Ok(false);
    }
    if owner_assertion {
        return Ok(true);
    }
    Ok(if generation == 0 {
        handoff::session_exit_blocker(tx, &agent)?.is_none()
    } else {
        capacity::has_owned_exit_and_reader_drain_tx(tx, num, generation)?
            && handoff::session_exit_blocker(tx, &agent)?.is_none()
    })
}

fn release_tx(
    tx: &Transaction<'_>,
    parent: &Command,
    pending: Vec<Command>,
    owner_reason: Option<&str>,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<CommandState> {
    let evidence = if owner_reason.is_some() {
        "owner"
    } else {
        "restart_custody"
    };
    for child in pending {
        let agent =
            parse_agent_id(child.target.as_deref().ok_or_else(|| {
                Error::new("source_integrity", "switch child has no target Agent")
            })?)?;
        let generation = child
            .generation
            .ok_or_else(|| Error::new("source_integrity", "switch child has no generation"))?;
        cancel_pending_command_tx(
            tx,
            agent,
            generation,
            &child.id,
            "interrupted_switch",
            now,
            events,
        )?;
        if child.verb == "launch" {
            capacity::release_unstarted_tx(tx, agent, 1, &child.id)?;
            fail_unstarted_launch_tx(tx, agent, &child.id, now, events)?;
        }
    }
    let mut value = stored_result(&parent.outcome)?;
    if parent.state == CommandState::Unknown {
        value.state = CommandState::Failed;
        value.error = Some("switch_checkout_released".to_owned());
        value.error_reason = Some(
            owner_reason
                .unwrap_or("Restart proved the checkout writers exited.")
                .to_owned(),
        );
        value.next_action.clear();
        let outcome = serde_json::to_value(&value)
            .map_err(|_| Error::new("internal", "cannot encode switch result"))?;
        update_command_state(tx, &parent.id, value.state, Some(&outcome), None, now)?;
        Store::settle_automatic_handoff_attempt_tx(tx, &parent.id, value.state, now)?;
    } else {
        value.state = parent.state;
    }
    tx.execute(
        "DELETE FROM switch_checkout_owners WHERE switch_command = ?1",
        [&parent.id],
    )
    .map_err(sql_error)?;
    events.push(insert_daemon_command_event(
        tx,
        now,
        "switch.checkout_released",
        None,
        None,
        &parent.id,
        json!({"command_id": parent.id, "resolution_source": evidence, "reason": owner_reason,
            "state": command_state_name(value.state)}),
    )?);
    Ok(value.state)
}
