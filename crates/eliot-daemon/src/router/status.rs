//! Daemon status, retention and drain readbacks over the existing Router state.

use super::{Router, STATUS_BLOCKER_MAX};
use eliot_core::{
    Agent, AgentState, Command, CommandState, Error, Result, SecretPolicy,
    api::{
        AutonomyRequest, DaemonStatusResponse, DrainReport, OwnedProcessKind, OwnedProcessSummary,
        OwnerInboxKind, RetainedUnknownSummary, RetentionView, RunningTurnSummary, StatusBlocker,
        UnsettledCommandSummary, WorkspaceCustodyView,
    },
};
use eliot_plugin::sync::LockExt;
use serde_json::Value;
use std::collections::{HashMap, HashSet};

struct StatusStoreSnapshot {
    agents: Vec<Agent>,
    active_observers: Vec<bool>,
    unsettled_commands: Vec<Command>,
    unsettled_total: i64,
    live_owners: Vec<eliot_store::LiveProcessOwner>,
    live_owners_total: usize,
    write_faulted: bool,
    run_worker_rows: HashMap<String, Option<Command>>,
    non_retained_unknowns: Vec<String>,
    agents_total: i64,
    retained_page: Vec<Command>,
    retained_total: i64,
    retained_details: Vec<(bool, Option<Agent>)>,
    pruned_through: i64,
    space: Option<eliot_store::RetentionSpaceStatus>,
    agent_count: i64,
    status_unsettled: Vec<Command>,
    status_unsettled_total: i64,
    provider_probe_attention: Vec<eliot_core::ProviderHold>,
}

impl Router {
    async fn collect_status_snapshot(
        &self,
        need_drain: bool,
        need_blockers: bool,
        need_status: bool,
    ) -> Result<StatusStoreSnapshot> {
        self.clear_terminal_unknown_cache().await;
        let run_workers: Vec<(String, bool)> = if need_blockers {
            self.inner
                .run_workers
                .lock_recover()
                .iter()
                .map(|(id, worker)| (id.clone(), worker.task.is_finished()))
                .collect()
        } else {
            Vec::new()
        };
        let unknown_ids = if need_blockers {
            self.unknown_ids()
        } else {
            Vec::new()
        };
        let store = self.inner.store.lock().await;
        let agents = store.list_nonterminal_agents(STATUS_BLOCKER_MAX)?;
        let active_observers = if need_blockers {
            agents
                .iter()
                .map(|agent| {
                    if agent.observe_only {
                        store.observer_active(&agent.id, agent.generation)
                    } else {
                        Ok(false)
                    }
                })
                .collect::<Result<Vec<_>>>()?
        } else {
            Vec::new()
        };
        let unsettled_commands =
            store.list_unsettled_commands_excluding_retained(STATUS_BLOCKER_MAX)?;
        let unsettled_total = store.count_unsettled_commands_excluding_retained()?;
        let live_limit = if need_drain { STATUS_BLOCKER_MAX } else { 1 };
        let (live_owners, live_owners_total) = store.live_process_owners(live_limit)?;
        let write_faulted = store.is_write_faulted();

        let mut unsettled_ids = HashSet::new();
        for command in &unsettled_commands {
            unsettled_ids.insert(command.id.clone());
        }
        let mut run_worker_rows = HashMap::new();
        for (run_id, finished) in run_workers {
            if finished || unsettled_ids.contains(&run_id) {
                continue;
            }
            let row = store.get_command(&run_id)?;
            if let Some(command) = &row
                && matches!(
                    command.state,
                    CommandState::Accepted
                        | CommandState::Queued
                        | CommandState::Dispatched
                        | CommandState::Unknown
                )
            {
                unsettled_ids.insert(command.id.clone());
            }
            run_worker_rows.insert(run_id, row);
        }
        let mut non_retained_unknowns = Vec::new();
        for id in unknown_ids {
            if unsettled_ids.contains(&id) {
                continue;
            }
            if store.is_retained_unknown(&id).unwrap_or(false) {
                continue;
            }
            non_retained_unknowns.push(id);
        }

        let (agents_total, retained_page, retained_total, retained_details) = if need_drain {
            let agents_total = store.count_nonterminal_agents()?;
            let retained_page = store.list_retained_unknown_commands(STATUS_BLOCKER_MAX)?;
            let retained_total = store.count_retained_unknown_commands()?;
            let mut details = Vec::with_capacity(retained_page.len());
            for command in &retained_page {
                let (still_retained, agent) = if let Some(target) = &command.target
                    && command.generation.is_some()
                {
                    let retained = store.is_retained_unknown(&command.id).unwrap_or(false);
                    let agent = if retained {
                        store.get_agent(target).ok().flatten()
                    } else {
                        None
                    };
                    (retained, agent)
                } else {
                    (false, None)
                };
                details.push((still_retained, agent));
            }
            (agents_total, retained_page, retained_total, details)
        } else {
            (0, Vec::new(), 0, Vec::new())
        };

        let (
            pruned_through,
            space,
            agent_count,
            status_unsettled,
            status_unsettled_total,
            provider_probe_attention,
        ) = if need_status {
            (
                store.retention_watermark()?,
                Some(store.retention_space_status()?),
                store.agent_count()?,
                store.list_unsettled_commands(STATUS_BLOCKER_MAX)?,
                store.count_unsettled_commands()?,
                store.provider_probe_attention(STATUS_BLOCKER_MAX)?,
            )
        } else {
            (0, None, 0, Vec::new(), 0, Vec::new())
        };

        Ok(StatusStoreSnapshot {
            agents,
            active_observers,
            unsettled_commands,
            unsettled_total,
            live_owners,
            live_owners_total,
            write_faulted,
            run_worker_rows,
            non_retained_unknowns,
            agents_total,
            retained_page,
            retained_total,
            retained_details,
            pruned_through,
            space,
            agent_count,
            status_unsettled,
            status_unsettled_total,
            provider_probe_attention,
        })
    }

    fn build_blockers_from_snapshot(&self, snap: &StatusStoreSnapshot) -> Vec<StatusBlocker> {
        let mut blockers = Vec::new();
        if self.inner.workspaces.has_owned_processes() || snap.live_owners_total > 0 {
            blockers.push(StatusBlocker::WorkspaceProcesses);
        }
        let discovery_readers = self.inner.plugin_host.reader_count();
        if discovery_readers > 0 {
            blockers.push(StatusBlocker::DiscoveryReaders {
                count: discovery_readers,
            });
        }
        for (agent, source_active) in snap.agents.iter().zip(&snap.active_observers) {
            let observer_present = self
                .inner
                .plugin_host
                .observes_generation(&agent.id, agent.generation);
            if agent.observe_only
                && agent.state == AgentState::Unknown
                && !*source_active
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
        for command in &snap.unsettled_commands {
            unsettled_ids.insert(command.id.clone());
            blockers.push(StatusBlocker::CommandUnsettled {
                command_id: command.id.clone(),
                state: command.state,
            });
        }
        for (run_id, row) in &snap.run_worker_rows {
            let live = row.as_ref().is_some_and(|command| {
                matches!(
                    command.state,
                    CommandState::Accepted
                        | CommandState::Queued
                        | CommandState::Dispatched
                        | CommandState::Unknown
                )
            });
            match row.as_ref().filter(|_| live) {
                Some(command) => {
                    if unsettled_ids.len() < STATUS_BLOCKER_MAX {
                        unsettled_ids.insert(command.id.clone());
                        blockers.push(StatusBlocker::CommandUnsettled {
                            command_id: command.id.clone(),
                            state: command.state,
                        });
                    }
                }
                None => blockers.push(StatusBlocker::ScriptRunActive {
                    run_id: run_id.clone(),
                }),
            }
        }
        let unsettled_total = usize::try_from(snap.unsettled_total.max(0)).unwrap_or(usize::MAX);
        if unsettled_total > unsettled_ids.len() {
            blockers.push(StatusBlocker::CommandUnsettledMore {
                count: unsettled_total - unsettled_ids.len(),
            });
        }
        if snap.write_faulted {
            blockers.push(StatusBlocker::StoreWriteFaulted);
        }
        for id in snap.non_retained_unknowns.iter().take(STATUS_BLOCKER_MAX) {
            blockers.push(StatusBlocker::CommandUnknown {
                command_id: id.clone(),
            });
        }
        if snap.non_retained_unknowns.len() > STATUS_BLOCKER_MAX {
            blockers.push(StatusBlocker::CommandUnknownMore {
                count: snap.non_retained_unknowns.len() - STATUS_BLOCKER_MAX,
            });
        }
        blockers
    }

    fn build_drain_from_snapshot(&self, snap: &StatusStoreSnapshot) -> DrainReport {
        let mut retained_unresolved = Vec::new();
        for (command, (still_retained, agent)) in
            snap.retained_page.iter().zip(&snap.retained_details)
        {
            if !*still_retained {
                continue;
            }
            let Some(target) = command.target.as_deref() else {
                continue;
            };
            let Some(generation) = command.generation else {
                continue;
            };
            if self
                .inner
                .plugin_host
                .observes_generation(target, generation)
            {
                continue;
            }
            let Some(agent) = agent.as_ref() else {
                continue;
            };
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
            reason = eliot_core::bound_chars(&reason, 128);
            let resolvable = !agent.observe_only;
            let hint = if resolvable {
                "owner-resolvable with cmd resolve (command) or turn resolve (turn); retained for the record; use --force only for live work".to_owned()
            } else {
                "not owner-resolvable (observe-only commands cannot be resolved); retained for the record, no action needed; use --force only for live work".to_owned()
            };
            retained_unresolved.push(RetainedUnknownSummary {
                id: command.id.clone(),
                verb: command.verb.clone(),
                agent: command.target.clone(),
                generation: command.generation,
                reason,
                resolvable,
                hint,
            });
        }
        retained_unresolved.sort_by(|left, right| left.id.cmp(&right.id));
        let agents_total = usize::try_from(snap.agents_total.max(0)).unwrap_or(usize::MAX);
        let unsettled_total = usize::try_from(snap.unsettled_total.max(0)).unwrap_or(usize::MAX);
        let running_turns_truncated = snap.agents.len() < agents_total;
        let mut running_turns = snap
            .agents
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
        let unsettled_details = snap
            .unsettled_commands
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
        let mut owned_processes = snap
            .live_owners
            .iter()
            .map(|owner| OwnedProcessSummary {
                agent: owner.agent.clone(),
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
        let owned_processes_truncated = owned_processes.len() < snap.live_owners_total;
        owned_processes.truncate(STATUS_BLOCKER_MAX);
        let owned_processes_count = owned_processes.len();
        let retained_unresolved_count =
            usize::try_from(snap.retained_total.max(0)).unwrap_or(usize::MAX);
        let retained_unresolved_truncated = retained_unresolved.len() < retained_unresolved_count;
        retained_unresolved.truncate(STATUS_BLOCKER_MAX);
        DrainReport {
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
        }
    }

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
        let claude_profile_command_issues = self.claude_profile_command_issues(None).await;
        let config = crate::config::load_daemon_config(&self.inner.data_root);
        let (db_bytes, wal_bytes) = crate::maintenance::store_file_sizes(&self.inner.data_root);
        let snap = self.collect_status_snapshot(true, true, true).await?;
        let blockers = self.build_blockers_from_snapshot(&snap);
        let drain = self.build_drain_from_snapshot(&snap);
        let retention = Some(retention_view_of(
            config.map(|config| config.retention),
            snap.pruned_through,
            snap.space.expect("status snapshot loads retention space"),
            db_bytes,
            wal_bytes,
            &self.secret_policy_snapshot(),
        ));
        let mode = self.mode_name();
        let mut unknown_commands = self.unknown_ids();
        let unknown_commands_count = unknown_commands.len();
        let unknown_commands_truncated = unknown_commands.len() > STATUS_BLOCKER_MAX;
        unknown_commands.truncate(STATUS_BLOCKER_MAX);
        let unsettled_total =
            usize::try_from(snap.status_unsettled_total.max(0)).unwrap_or(usize::MAX);
        let unsettled_commands_truncated = snap.status_unsettled.len() < unsettled_total;
        let unsettled_command_details = snap
            .status_unsettled
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
            provider_probe_attention: snap.provider_probe_attention,
            state: if mode == "open" { "running" } else { mode }.to_owned(),
            admission: mode.to_owned(),
            agents: usize::try_from(snap.agent_count)
                .map_err(|_| Error::new("internal", "Agent count is invalid"))?,
            store_write_faulted: snap.write_faulted,
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
            unsettled_commands_count: snap.status_unsettled.len(),
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

    /// Bounded read-side drain report reusing the existing shutdown blockers'
    /// sources.
    pub(super) async fn drain_report(&self) -> Result<DrainReport> {
        let snap = self.collect_status_snapshot(true, false, false).await?;
        Ok(self.build_drain_from_snapshot(&snap))
    }

    pub(super) async fn status_blockers(&self) -> Result<Vec<StatusBlocker>> {
        let snap = self.collect_status_snapshot(false, true, false).await?;
        Ok(self.build_blockers_from_snapshot(&snap))
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
