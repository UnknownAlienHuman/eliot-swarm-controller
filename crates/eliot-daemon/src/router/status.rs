//! Daemon status, retention and drain readbacks over the existing Router state.

use super::{Router, STATUS_BLOCKER_MAX};
use eliot_core::{
    AgentState, Command, CommandState, Error, Result, SecretPolicy,
    api::{
        AutonomyRequest, DaemonStatusResponse, DrainReport, OwnedProcessKind, OwnedProcessSummary,
        OwnerInboxKind, RetainedUnknownSummary, RetentionView, RunningTurnSummary, StatusBlocker,
        UnsettledCommandSummary, WorkspaceCustodyView,
    },
};
use eliot_plugin::sync::LockExt;
use serde_json::Value;
use std::collections::HashSet;

impl Router {
    pub(super) async fn autonomy(&self, request: AutonomyRequest) -> Result<Value> {
        let until = super::now_ms();
        let since = request.since(until)?;
        let permit = self
            .inner
            .autonomy_reader
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::new("unavailable", "an autonomy report is already running"))?;
        let path = self.inner.data_root.join("eliot.db");
        let confirmed = self.confirmed_stuck();
        let report = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            eliot_store::Store::read_autonomy(&path, since, until, &confirmed)
        })
        .await
        .map_err(|_| Error::new("internal", "autonomy reader failed"))??;
        serde_json::to_value(report)
            .map_err(|_| Error::new("internal", "autonomy report could not be encoded"))
    }

    pub(super) async fn status(&self) -> Result<Value> {
        let (inbox, liveness) = self.read_owner_projection().await?;
        let mut owner_inbox = OwnerInboxKind::ALL
            .into_iter()
            .map(|kind| (kind, 0))
            .collect::<std::collections::BTreeMap<_, _>>();
        for item in inbox {
            *owner_inbox.entry(item.kind).or_default() += 1;
        }
        let claude_profile_command_issues = self.claude_profile_command_issues().await;
        let blockers = self.status_blockers().await?;
        let drain = self.drain_report().await?;
        let retention = Some(self.retention_view().await?);
        let mode = self.mode_name();
        let (
            agent_count,
            write_faulted,
            unsettled_commands,
            unsettled_total,
            provider_probe_attention,
        ) = {
            let store = self.inner.store.lock().await;
            (
                store.agent_count()?,
                store.is_write_faulted(),
                store.list_unsettled_commands(STATUS_BLOCKER_MAX)?,
                store.count_unsettled_commands()?,
                store.provider_probe_attention(STATUS_BLOCKER_MAX)?,
            )
        };
        let mut unknown_commands = self.unknown_ids();
        let unknown_commands_count = unknown_commands.len();
        let unknown_commands_truncated = unknown_commands.len() > STATUS_BLOCKER_MAX;
        unknown_commands.truncate(STATUS_BLOCKER_MAX);
        let unsettled_total = usize::try_from(unsettled_total.max(0)).unwrap_or(usize::MAX);
        let unsettled_commands_truncated = unsettled_commands.len() < unsettled_total;
        let unsettled_command_details = unsettled_commands
            .iter()
            .map(|command| UnsettledCommandSummary {
                id: command.id.clone(),
                agent: command.target.clone(),
                generation: command.generation,
                verb: command.verb.clone(),
                state: command.state,
            })
            .collect::<Vec<_>>();
        let workspace_custody: WorkspaceCustodyView =
            serde_json::from_value(crate::workspaces::custody_capability())
                .map_err(|_| Error::new("internal", "workspace custody view is invalid"))?;
        let policy = self.secret_policy_snapshot();
        let (plugin_crashes, plugin_crashes_truncated) = {
            let mut status = self.inner.plugin_crashes.lock_recover();
            let markers = crate::crash::take_plugin_crashes(&self.inner.data_root, &policy);
            if !markers.is_empty() {
                status.markers.extend(markers);
                status.markers.sort_by_key(|marker| marker.timestamp);
                let excess = status
                    .markers
                    .len()
                    .saturating_sub(DaemonStatusResponse::MAX_PLUGIN_CRASHES);
                status.markers.drain(..excess);
                status.truncated |= excess > 0;
            }
            (status.markers.clone(), status.truncated)
        };
        serde_json::to_value(DaemonStatusResponse {
            owner_inbox,
            liveness,
            build_identity: Some(crate::build_identity::current().clone()),
            provider_probe_attention,
            state: if mode == "open" { "running" } else { mode }.to_owned(),
            admission: mode.to_owned(),
            agents: usize::try_from(agent_count)
                .map_err(|_| Error::new("internal", "Agent count is invalid"))?,
            store_write_faulted: write_faulted,
            owned_plugins: self.inner.plugin_host.live_count(),
            observer_readers: self.inner.plugin_host.observer_count(),
            discovery_readers: self.inner.plugin_host.reader_count(),
            claude_profile_command_issues,
            workspace_custody,
            workspace_recovery_error: self.inner.workspace_recovery_error.lock_recover().clone(),
            cloud_mcp: crate::cloud_mcp::service_snapshot(),
            automation_tasks: self.inner.worker_tasks.lock_recover().clone(),
            automatic_cleanup: Some(self.automatic_cleanup_view()),
            unknown_commands_count,
            unknown_commands,
            unknown_commands_truncated,
            unsettled_commands_count: unsettled_commands.len(),
            unsettled_commands: unsettled_command_details,
            unsettled_commands_truncated,
            blockers,
            drain: Some(drain),
            retention,
            plugin_crashes,
            plugin_crashes_truncated,
            previous_crash: self.inner.previous_crash.lock_recover().clone(),
        })
        .map_err(|_| Error::new("internal", "daemon status could not be encoded"))
    }

    /// Retention and store-size visibility for `daemon.status`: the effective
    /// windows, SQLite auto-vacuum/freelist state, the retained-event
    /// watermark, the main+WAL file sizes, and the unbounded-growth advisory.
    /// Status stays available as the diagnostic surface when `config.toml`
    /// cannot be read, but the windows are then unknown: the view carries the
    /// typed `config_error` and never reports an unset (unbounded) policy or
    /// its advisory. The configured tick logs the rejection separately.
    async fn retention_view(&self) -> Result<RetentionView> {
        let config = crate::config::load_daemon_config(&self.inner.data_root);
        let (pruned_through, space) = {
            let store = self.inner.store.lock().await;
            (
                store.retention_watermark()?,
                store.retention_space_status()?,
            )
        };
        let (db_bytes, wal_bytes) = crate::maintenance::store_file_sizes(&self.inner.data_root);
        Ok(retention_view_of(
            config.map(|config| config.retention),
            pruned_through,
            space,
            db_bytes,
            wal_bytes,
            &self.secret_policy_snapshot(),
        ))
    }

    /// Retained unknowns that no longer block drain: stored `unknown`
    /// commands with no live process, turn or reader behind them. An open
    /// source row counts as a reader only while its generation is the
    /// agent's current live generation; rows on fenced or terminal
    /// generations are residue. The Store classifies them with one SQL
    /// predicate over durable facts, exhaustive over all stored unknowns
    /// (no page cap, no id lists), so unknowns the daemon never saw
    /// dispatched (for example across a restart, where the in-memory cache
    /// starts empty) are still classified however old they are. The daemon
    /// only adds its live plugin-reader check, which is not a durable fact.
    /// The stored outcome stays unknown; only the drain wait is retired, and
    /// the drain receipt names each one with its resolvability and owner
    /// action. Returns the bounded receipt page plus the exact total.
    async fn retained_unresolved_unknowns(&self) -> Result<(Vec<RetainedUnknownSummary>, i64)> {
        let (page, total) = {
            let store = self.inner.store.lock().await;
            (
                store.list_retained_unknown_commands(STATUS_BLOCKER_MAX)?,
                store.count_retained_unknown_commands()?,
            )
        };
        let mut retained = Vec::new();
        for command in &page {
            if let Some(summary) = self.retained_summary(command).await {
                retained.push(summary);
            }
        }
        retained.sort_by(|left, right| left.id.cmp(&right.id));
        Ok((retained, total))
    }

    /// Receipt entry for one SQL-classified retained unknown. Re-verifies
    /// the predicate at summary time and returns `None` while a live plugin
    /// reader still observes the generation: the in-memory reader is not a
    /// durable fact, so it stays a daemon-side block, never a silent excuse.
    async fn retained_summary(&self, command: &Command) -> Option<RetainedUnknownSummary> {
        let target = command.target.clone()?;
        let generation = command.generation?;
        if self
            .inner
            .plugin_host
            .observes_generation(&target, generation)
        {
            return None;
        }
        let agent = {
            let store = self.inner.store.lock().await;
            // The page above and this read race live state; only a
            // still-retained row may retire the drain wait.
            if !store.is_retained_unknown(&command.id).ok()? {
                return None;
            }
            store.get_agent(&target).ok().flatten()?
        };
        // The predicate already encodes the durable turn checks (only a
        // recorded turn matching the live turn backs the command, and a
        // live turn on the same generation blocks); the plugin check above
        // covers the non-durable reader.
        let mut reason = command
            .outcome
            .as_ref()
            .and_then(|outcome| outcome.get("reason"))
            .and_then(Value::as_str)
            .map(str::to_owned)
            .filter(|reason| !reason.trim().is_empty())
            .or_else(|| {
                self.unknown_cause(&command.id)
                    .filter(|cause| !cause.trim().is_empty())
            })
            .unwrap_or_else(|| "unknown".to_owned());
        // Floor to a character boundary first: the old truncate-then-fix-up
        // ran after the panic. 'bound_chars' never splits UTF-8 mid-codepoint.
        reason = eliot_core::bound_chars(&reason, 128);
        let resolvable = !agent.observe_only;
        let hint = if resolvable {
            "owner-resolvable with cmd resolve (command) or turn resolve (turn); retained for the record; use --force only for live work".to_owned()
        } else {
            "not owner-resolvable (observe-only commands cannot be resolved); retained for the record, no action needed; use --force only for live work".to_owned()
        };
        Some(RetainedUnknownSummary {
            id: command.id.clone(),
            verb: command.verb.clone(),
            agent: command.target.clone(),
            generation: command.generation,
            reason,
            resolvable,
            hint,
        })
    }

    /// Bounded read-side drain report reusing the existing shutdown blockers'
    /// sources. Running turns come from nonterminal Agents with a recorded
    /// active turn; unsettled/unknown reuse the status projections; owned
    /// processes come from the Store custody snapshot (process records not
    /// yet exited), never from routing eligibility or workspace operation
    /// locks. Every category carries a count, a bounded list and a truncation
    /// flag that is true whenever any qualifying record was omitted for any
    /// reason: the list bound or the Store page byte bound trimming the page.
    /// Unknown stays unknown: an empty report never proves no native effect
    /// occurred. Stored unknowns with no live process, turn or reader are
    /// listed under `retained_unresolved` with their resolvability and owner
    /// action instead of blocking: the drain wait is retired, never the
    /// outcome. The unsettled section excludes the same predicate in SQL
    /// before the bound so the receipt never double-lists them; its count
    /// covers the excluding page.
    pub(super) async fn drain_report(&self) -> Result<DrainReport> {
        self.clear_terminal_unknown_cache().await;
        // Retained unknowns leave the unsettled section in SQL before the
        // bound (one predicate over durable facts, no id lists), so they can
        // neither occupy the bounded prefix nor hide a live command in the
        // receipt.
        let (mut retained_unresolved, retained_total) = self.retained_unresolved_unknowns().await?;
        let (agents, agents_total, unsettled_commands, unsettled_total, custody) = {
            let store = self.inner.store.lock().await;
            (
                store.list_nonterminal_agents(STATUS_BLOCKER_MAX)?,
                store.count_nonterminal_agents()?,
                store.list_unsettled_commands_excluding_retained(STATUS_BLOCKER_MAX)?,
                store.count_unsettled_commands_excluding_retained()?,
                store.live_process_owners(STATUS_BLOCKER_MAX)?,
            )
        };
        let (live_owners, live_total) = custody;
        let agents_total = usize::try_from(agents_total.max(0)).unwrap_or(usize::MAX);
        let unsettled_total = usize::try_from(unsettled_total.max(0)).unwrap_or(usize::MAX);
        // Running turns derive solely from the Agent page: any omitted Agent
        // may hide a turn, so the Agent omission decides this flag.
        let running_turns_truncated = agents.len() < agents_total;
        let mut running_turns = agents
            .iter()
            .filter_map(|agent| {
                agent
                    .active_turn
                    .as_ref()
                    .map(|turn_id| RunningTurnSummary {
                        agent: agent.id.clone(),
                        generation: agent.generation,
                        turn_id: turn_id.clone(),
                    })
            })
            .collect::<Vec<_>>();
        running_turns.sort_by(|left, right| {
            (&left.agent, left.generation, &left.turn_id).cmp(&(
                &right.agent,
                right.generation,
                &right.turn_id,
            ))
        });
        running_turns.truncate(STATUS_BLOCKER_MAX);
        let running_turns_count = running_turns.len();
        // The unsettled section mirrors the blockers: retained entries were
        // already excluded in SQL and live under `retained_unresolved`
        // instead of double-listing here. The count covers the listed page
        // and the flag stays true whenever the excluding store total exceeds
        // it for any reason.
        let unsettled_details = unsettled_commands
            .iter()
            .map(|command| UnsettledCommandSummary {
                id: command.id.clone(),
                agent: command.target.clone(),
                generation: command.generation,
                verb: command.verb.clone(),
                state: command.state,
            })
            .collect::<Vec<_>>();
        let unsettled_commands_count = unsettled_details.len();
        let unsettled_commands_truncated = unsettled_details.len() < unsettled_total;
        let mut unknown_commands = self.unknown_ids();
        let unknown_commands_count = unknown_commands.len();
        let unknown_commands_truncated = unknown_commands.len() > STATUS_BLOCKER_MAX;
        unknown_commands.truncate(STATUS_BLOCKER_MAX);
        let mut owned_processes = live_owners
            .into_iter()
            .map(|owner| OwnedProcessSummary {
                agent: owner.agent,
                generation: owner.generation,
                kind: if owner.workspace {
                    OwnedProcessKind::Workspace
                } else {
                    OwnedProcessKind::Plugin
                },
            })
            .collect::<Vec<_>>();
        owned_processes.sort_by(|left, right| {
            (&left.agent, left.generation, left.kind as u8).cmp(&(
                &right.agent,
                right.generation,
                right.kind as u8,
            ))
        });
        let owned_processes_truncated = owned_processes.len() < live_total;
        owned_processes.truncate(STATUS_BLOCKER_MAX);
        let owned_processes_count = owned_processes.len();
        // The total is the exact SQL count over all stored unknowns; the
        // flag stays true whenever the bounded (or reader-filtered) page
        // omits any of it.
        let retained_unresolved_count =
            usize::try_from(retained_total.max(0)).unwrap_or(usize::MAX);
        let retained_unresolved_truncated = retained_unresolved.len() < retained_unresolved_count;
        retained_unresolved.truncate(STATUS_BLOCKER_MAX);
        Ok(DrainReport {
            running_turns_count,
            running_turns,
            running_turns_truncated,
            unsettled_commands_count,
            unsettled_commands: unsettled_details,
            unsettled_commands_truncated,
            unknown_commands_count,
            unknown_commands,
            unknown_commands_truncated,
            owned_processes_count,
            owned_processes,
            owned_processes_truncated,
            retained_unresolved_count,
            retained_unresolved,
            retained_unresolved_truncated,
        })
    }

    pub(super) async fn status_blockers(&self) -> Result<Vec<StatusBlocker>> {
        self.clear_terminal_unknown_cache().await;
        // Retained unknowns retire the drain wait everywhere: the Store
        // excludes the predicate in SQL before the unsettled bound, so
        // retained entries can neither occupy a blocker slot nor hide a
        // live command behind the bounded prefix. The excluding total drives
        // the overflow fallback below: any command the bound omits keeps
        // blocking, while a fleet of only retained unknowns still drains
        // because the predicate already excluded every one of them.
        let (
            agents,
            active_observers,
            unsettled_commands,
            unsettled_total,
            live_owners_total,
            write_faulted,
        ) = {
            let store = self.inner.store.lock().await;
            let agents = store.list_nonterminal_agents(STATUS_BLOCKER_MAX)?;
            let active_observers = agents
                .iter()
                .map(|agent| {
                    if agent.observe_only {
                        store.observer_active(&agent.id, agent.generation)
                    } else {
                        Ok(false)
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            let unsettled_commands =
                store.list_unsettled_commands_excluding_retained(STATUS_BLOCKER_MAX)?;
            let unsettled_total = store.count_unsettled_commands_excluding_retained()?;
            // Store custody is the owned-process source of truth beside the
            // workspace slots: a live owned process record blocks even when
            // no slot claims it.
            let live_owners_total = store.live_process_owners(1)?.1;
            (
                agents,
                active_observers,
                unsettled_commands,
                unsettled_total,
                live_owners_total,
                store.is_write_faulted(),
            )
        };
        let mut blockers = Vec::new();
        if self.inner.workspaces.has_owned_processes() || live_owners_total > 0 {
            blockers.push(StatusBlocker::WorkspaceProcesses);
        }
        let discovery_readers = self.inner.plugin_host.reader_count();
        if discovery_readers > 0 {
            blockers.push(StatusBlocker::DiscoveryReaders {
                count: discovery_readers,
            });
        }
        for (agent, source_active) in agents.into_iter().zip(active_observers) {
            let observer_present = self
                .inner
                .plugin_host
                .observes_generation(&agent.id, agent.generation);
            if agent.observe_only
                && agent.state == AgentState::Unknown
                && !source_active
                && !observer_present
            {
                continue;
            }
            blockers.push(StatusBlocker::AgentLive {
                agent: agent.id.clone(),
                generation: agent.generation,
                state: agent.state,
                active_turn: agent.active_turn.clone(),
            });
        }
        let mut unsettled_ids = HashSet::new();
        for command in unsettled_commands {
            unsettled_ids.insert(command.id.clone());
            blockers.push(StatusBlocker::CommandUnsettled {
                command_id: command.id,
                state: command.state,
            });
        }
        // Live script Run workers gate drain explicitly: the worker registry
        // is separate from the command rows, and a worker can outlive its
        // row settlement while output admission finishes. Workers whose
        // command is still unsettled are already listed above (or counted in
        // the overflow remainder below when the page omits them); only the
        // settled-row remainder needs its own entry, and every unfinished
        // worker either appears here or is counted, never silently dropped.
        let run_workers: Vec<(String, bool)> = self
            .inner
            .run_workers
            .lock_recover()
            .iter()
            .map(|(id, worker)| (id.clone(), worker.task.is_finished()))
            .collect();
        for (run_id, finished) in run_workers {
            if finished || unsettled_ids.contains(&run_id) {
                continue;
            }
            let row = {
                let store = self.inner.store.lock().await;
                store.get_command(&run_id)?
            };
            let live = row.as_ref().is_some_and(|command| {
                matches!(
                    command.state,
                    CommandState::Accepted
                        | CommandState::Queued
                        | CommandState::Dispatched
                        | CommandState::Unknown
                )
            });
            match row.filter(|_| live) {
                Some(command) => {
                    // The excluding total below counts this row whether or
                    // not the bounded page lists it, so only fill a still
                    // free slot: the remainder counts exactly the omitted.
                    if unsettled_ids.len() < STATUS_BLOCKER_MAX {
                        unsettled_ids.insert(command.id.clone());
                        blockers.push(StatusBlocker::CommandUnsettled {
                            command_id: command.id,
                            state: command.state,
                        });
                    }
                }
                None => blockers.push(StatusBlocker::ScriptRunActive { run_id }),
            }
        }
        // The bounded list is diagnostic: the excluding total is the drain
        // decision, so any live command the bound omits keeps blocking with
        // its exact unlisted remainder. The remainder counts only entries
        // the list omits: every listed id above is already in the total.
        let unsettled_total = usize::try_from(unsettled_total.max(0)).unwrap_or(usize::MAX);
        if unsettled_total > unsettled_ids.len() {
            blockers.push(StatusBlocker::CommandUnsettledMore {
                count: unsettled_total - unsettled_ids.len(),
            });
        }
        if write_faulted {
            blockers.push(StatusBlocker::StoreWriteFaulted);
        }
        // Filter retained and already-unsettled ids before the list bound and
        // the overflow calculation, so retired entries can neither occupy a
        // blocker slot nor trip the overflow counter. Retention is decided
        // per id by the same SQL predicate that excluded the unsettled
        // section; a Store error keeps the id blocking (fail closed).
        let mut blocking_unknown = Vec::new();
        {
            let store = self.inner.store.lock().await;
            for id in self.unknown_ids() {
                if unsettled_ids.contains(&id) {
                    continue;
                }
                if store.is_retained_unknown(&id).unwrap_or(false) {
                    continue;
                }
                blocking_unknown.push(id);
            }
        }
        for id in blocking_unknown.iter().take(STATUS_BLOCKER_MAX) {
            blockers.push(StatusBlocker::CommandUnknown {
                command_id: id.clone(),
            });
        }
        if blocking_unknown.len() > STATUS_BLOCKER_MAX {
            blockers.push(StatusBlocker::CommandUnknownMore {
                count: blocking_unknown.len() - STATUS_BLOCKER_MAX,
            });
        }
        Ok(blockers)
    }
}

/// The retention view for one configuration read: known windows, or the
/// typed read failure with unknown windows (never unbounded, no advisory).
fn retention_view_of(
    config: Result<crate::config::RetentionConfig>,
    pruned_through: i64,
    space: eliot_store::RetentionSpaceStatus,
    db_bytes: u64,
    wal_bytes: u64,
    policy: &SecretPolicy,
) -> RetentionView {
    let (retention, config_error) = match config {
        Ok(retention) => (retention, None),
        Err(error) => {
            let error = error.sanitized(policy);
            let mut line = format!("{}: {}", error.code, error.message);
            if line.len() > 512 {
                let mut end = 509;
                while !line.is_char_boundary(end) {
                    end -= 1;
                }
                line.truncate(end);
                line.push_str("...");
            }
            (crate::config::RetentionConfig::default(), Some(line))
        }
    };
    let known = config_error.is_none();
    let events_seconds = known.then(|| retention.events_retention_seconds());
    let processes_seconds = known.then(|| retention.processes_retention_seconds());
    let maintenance_interval_seconds =
        known.then(|| retention.maintenance_interval_seconds() as i64);
    let compact_on_start = known.then_some(retention.compact_on_start);
    let unbounded = known
        && events_seconds.is_none()
        && processes_seconds.is_none()
        && retention.commands_days.is_none()
        && retention.mail_days.is_none();
    let size_advisory = if unbounded
        && db_bytes.saturating_add(wal_bytes)
            > crate::config::RETENTION_UNBOUNDED_SIZE_ADVISORY_BYTES
    {
        Some("retention is unbounded and the store exceeds 100 MiB; set daemon.retention windows (see configuration.md)".to_owned())
    } else {
        None
    };
    RetentionView {
        events_days: known.then_some(retention.events_retention_days()).flatten(),
        commands_days: retention.commands_days,
        mail_days: retention.mail_days,
        events_seconds,
        processes_seconds,
        maintenance_interval_seconds,
        compact_on_start,
        auto_vacuum_mode: space.auto_vacuum_mode,
        page_count: space.page_count,
        freelist_pages: space.freelist_pages,
        pruned_through,
        db_bytes,
        wal_bytes,
        unbounded,
        size_advisory,
        config_error,
    }
}
