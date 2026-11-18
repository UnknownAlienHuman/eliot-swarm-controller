//! Daemon status, retention and drain readbacks over the existing Router state.

use super::{Router, STATUS_BLOCKER_MAX};
use eliot_core::{
    AgentState, Command, CommandState, Error, Result, SecretPolicy,
    api::{
        DaemonStatusResponse, DrainReport, OwnedProcessKind, OwnedProcessSummary,
        RetainedUnknownSummary, RetentionView, RunningTurnSummary, StatusBlocker,
        UnsettledCommandSummary, WorkerTaskStatus, WorkspaceCustodyView,
    },
};
use eliot_plugin::sync::LockExt;
use serde_json::Value;
use std::collections::HashSet;

impl Router {
    pub(super) async fn status(&self) -> Result<Value> {
        let blockers = self.status_blockers().await?;
        let drain = self.drain_report().await?;
        let retention = Some(self.retention_view().await?);
        let mode = self.mode_name();
        let (agent_count, write_faulted, unsettled_commands, unsettled_total) = {
            let store = self.inner.store.lock().await;
            (
                store.agent_count()?,
                store.is_write_faulted(),
                store.list_unsettled_commands(STATUS_BLOCKER_MAX)?,
                store.count_unsettled_commands()?,
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
        serde_json::to_value(DaemonStatusResponse {
            state: if mode == "open" { "running" } else { mode }.to_owned(),
            admission: mode.to_owned(),
            agents: usize::try_from(agent_count)
                .map_err(|_| Error::new("internal", "Agent count is invalid"))?,
            store_write_faulted: write_faulted,
            owned_plugins: self.inner.plugin_host.live_count(),
            observer_readers: self.inner.plugin_host.observer_count(),
            discovery_readers: self.inner.plugin_host.reader_count(),
            workspace_custody,
            workspace_recovery_error: self.inner.workspace_recovery_error.lock_recover().clone(),
            cloud_mcp: crate::cloud_mcp::service_snapshot(),
            automation_tasks: self
                .inner
                .automation_tasks
                .lock_recover()
                .iter()
                .map(|(name, health, failure_code)| WorkerTaskStatus {
                    name: name.clone(),
                    health: *health,
                    failure_code: failure_code.clone(),
                })
                .collect(),
            unknown_commands_count,
            unknown_commands,
            unknown_commands_truncated,
            unsettled_commands_count: unsettled_commands.len(),
            unsettled_commands: unsettled_command_details,
            unsettled_commands_truncated,
            blockers,
            drain: Some(drain),
            retention,
            previous_crash: self.inner.previous_crash.lock_recover().clone(),
        })
        .map_err(|_| Error::new("internal", "daemon status could not be encoded"))
    }

    /// Retention and store-size visibility for `daemon.status`: the configured
    /// windows (or unbounded when all are unset), the retained-event
    /// watermark, the main+WAL file sizes, and the unbounded-growth advisory.
    /// Status stays available as the diagnostic surface when `config.toml`
    /// cannot be read, but the windows are then unknown: the view carries the
    /// typed `config_error` and never reports an unset (unbounded) policy or
    /// its advisory. The hourly tick logs the rejection separately.
    async fn retention_view(&self) -> Result<RetentionView> {
        let config = crate::config::load_daemon_config(&self.inner.data_root);
        let pruned_through = {
            let store = self.inner.store.lock().await;
            store.retention_watermark()?
        };
        let (db_bytes, wal_bytes) = crate::maintenance::store_file_sizes(&self.inner.data_root);
        Ok(retention_view_of(
            config.map(|config| config.retention),
            pruned_through,
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
    let unbounded = config_error.is_none()
        && retention.events_days.is_none()
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
        events_days: retention.events_days,
        commands_days: retention.commands_days,
        mail_days: retention.mail_days,
        pruned_through,
        db_bytes,
        wal_bytes,
        unbounded,
        size_advisory,
        config_error,
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{admit, result_fixture, router};
    use super::super::*;
    use super::*;

    #[tokio::test]
    async fn retained_unknown_reason_truncation_respects_char_boundary() {
        let router = router();
        let now = now_ms();
        let attach_id = {
            let mut store = router.inner.store.lock().await;
            let (agent, attach_id, detach_id) =
                fenced_observer_attach(&mut store, "reason-scope", "reason-native", now);
            close_and_settle_observer_detach(
                &mut store,
                &agent,
                &detach_id,
                "reason-scope",
                "reason-native",
                now,
            );
            // Byte 128 of the retained reason lands inside a Cyrillic char: the
            // old byte 'truncate' panicked here (the boundary fix-up ran after
            // truncation, too late).
            let reason = format!("x{}", "ж".repeat(100));
            store
                .settle(
                    &attach_id,
                    CommandState::Unknown,
                    Some(json!({"reason": reason})),
                    now,
                )
                .expect("re-settle unknown with a long reason");
            attach_id
        };
        let command = {
            let store = router.inner.store.lock().await;
            store
                .get_command(&attach_id)
                .expect("read back attach")
                .expect("attach row")
        };
        let summary = router
            .retained_summary(&command)
            .await
            .expect("retained unknown summary");
        assert!(summary.reason.len() <= 128);
        assert!(summary.reason.starts_with('x'));
    }
    #[tokio::test]
    async fn drain_report_covers_running_unsettled_unknown_and_owned() {
        let router = router();
        // Busy fixture Agent supplies the running turn; a second dispatched
        // launch supplies one unsettled command that we also track as
        // unknown. Fixtures have no live runtime, so owned processes stay
        // honestly empty.
        let (busy_agent, unsettled_id) = {
            let mut store = router.inner.store.lock().await;
            let (busy_agent, _) = result_fixture(&mut store);
            let launch_id = ulid::Ulid::new().to_string();
            store
                .submit(
                    "owner",
                    SubmitCommand {
                        id: launch_id.clone(),
                        target: None,
                        selector: None,
                        verb: "launch".to_owned(),
                        args: json!({
                            "project":"drain-report", "workspace":"synthetic-workspace",
                            "harness":"codex", "profile":"synthetic", "profile_snapshot":{},
                        }),
                        expected_generation: Some(0),
                        expected_turn_id: None,
                        request_id: None,
                        probe: false,
                    },
                    now_ms(),
                )
                .unwrap();
            store.dispatch(&launch_id, now_ms()).unwrap();
            (busy_agent, launch_id)
        };
        router
            .note_unknown_if_uncertain(&unsettled_id, "drain_report_test")
            .await;
        let shutdown = router
            .shutdown(
                &json!({"id":ulid::Ulid::new().to_string(), "mode":"drain"}),
                &Principal::Owner,
            )
            .await
            .unwrap();
        assert_eq!(shutdown["state"], json!("dispatched"));
        assert_eq!(shutdown["mode"], json!("drain"));
        let blockers = shutdown["blockers"].as_array().expect("blockers");
        assert!(
            blockers
                .iter()
                .any(|blocker| blocker["kind"] == json!("agent_live")
                    && blocker["agent"] == json!(busy_agent)
                    && blocker["active_turn"] == json!("turn-1"))
        );
        assert!(
            blockers
                .iter()
                .any(|blocker| blocker["kind"] == json!("command_unsettled")
                    && blocker["command_id"] == json!(unsettled_id))
        );
        let drain = shutdown["drain"].as_object().expect("drain report");
        let running = drain["running_turns"].as_array().expect("running turns");
        assert!(
            running.iter().any(
                |turn| turn["agent"] == json!(busy_agent) && turn["turn_id"] == json!("turn-1")
            )
        );
        assert_eq!(drain["running_turns_truncated"], json!(false));
        let unsettled = drain["unsettled_commands"]
            .as_array()
            .expect("unsettled commands");
        assert!(
            unsettled
                .iter()
                .any(|command| command["id"] == json!(unsettled_id))
        );
        assert_eq!(drain["unsettled_commands_truncated"], json!(false));
        let unknown = drain["unknown_commands"]
            .as_array()
            .expect("unknown commands");
        assert!(unknown.iter().any(|id| id == &json!(unsettled_id)));
        assert_eq!(drain["unknown_commands_truncated"], json!(false));
        // Fixtures have no live runtime or workspace child: the owned list is
        // honestly empty and not truncated. An empty report never proves no
        // native effect occurred.
        assert_eq!(drain["owned_processes"], json!([]));
        assert_eq!(drain["owned_processes_count"], json!(0));
        assert_eq!(drain["owned_processes_truncated"], json!(false));
        // Unknown stays unknown: drain never auto-resolves to failed. The
        // Store stays Dispatched (unsettled) while the in-memory unknown set
        // keeps the evidence-based unknown outcome.
        let store = router.inner.store.lock().await;
        assert_eq!(
            store.get_command(&unsettled_id).unwrap().unwrap().state,
            CommandState::Dispatched
        );
        assert!(router.unknown_ids().contains(&unsettled_id));
    }

    #[tokio::test]
    async fn drain_completes_with_retained_observer_unknown_listed() {
        let router = router();
        let now = now_ms();
        // Reproduce the detach-fence race: attach then immediately detach the
        // same native subject. The fence marks the dispatched attach unknown
        // with `observer_detach_fence`, and no owner resolution can retire
        // it (observe-only commands are refused by design).
        let (agent, attach_id, detach_id) = {
            let mut store = router.inner.store.lock().await;
            let admitted = store
                .submit(
                    "owner",
                    SubmitCommand {
                        id: ulid::Ulid::new().to_string(),
                        target: None,
                        selector: None,
                        verb: "attach".to_owned(),
                        args: json!({"mode":"observe","profile":"fixture","harness":"codex",
                            "native_scope":"fixture:scope","native_ref":"fixture-native",
                            "profile_snapshot":{"harness":"codex"}}),
                        expected_generation: None,
                        expected_turn_id: None,
                        request_id: None,
                        probe: false,
                    },
                    now,
                )
                .unwrap();
            let agent = admitted.command.target.unwrap();
            let attach_id = admitted.command.id.clone();
            store.dispatch(&attach_id, now).unwrap();
            let detach_id = ulid::Ulid::new().to_string();
            store
                .submit(
                    "owner",
                    SubmitCommand {
                        id: detach_id.clone(),
                        target: Some(agent.clone()),
                        selector: None,
                        verb: "detach".to_owned(),
                        args: json!({"mode":"observe"}),
                        expected_generation: Some(1),
                        expected_turn_id: None,
                        request_id: None,
                        probe: false,
                    },
                    now + 1,
                )
                .unwrap();
            store.dispatch(&detach_id, now + 1).unwrap();
            assert_eq!(
                store.get_command(&attach_id).unwrap().unwrap().state,
                CommandState::Unknown
            );
            // Confirmed plugin cleanup closes the retired observation source;
            // the detach transition itself stays live until it settles.
            store
                .close_native_sources(&agent, 1, now + 1)
                .expect("close retired observation");
            (agent, attach_id, detach_id)
        };
        router
            .note_unknown_if_uncertain(&attach_id, "observer_detach_fence")
            .await;
        // The detach transition itself still blocks drain while dispatched:
        // live work keeps `--force` as its only escape.
        let blocked = router
            .shutdown(
                &json!({"id":ulid::Ulid::new().to_string(), "mode":"drain"}),
                &Principal::Owner,
            )
            .await
            .unwrap();
        assert_eq!(blocked["state"], json!("dispatched"));
        assert!(
            blocked["blockers"]
                .as_array()
                .expect("blockers")
                .iter()
                .any(|blocker| blocker["kind"] == json!("command_unsettled")
                    && blocker["command_id"] == json!(detach_id))
        );
        // Reader teardown confirms the detach; the attach stays unknown.
        {
            let mut store = router.inner.store.lock().await;
            store
                .settle_observe_detach(
                    &detach_id,
                    json!({"agent_id":agent,"generation":2,"command_id":detach_id,
                        "native_scope":"fixture:scope","native_ref":"fixture-native",
                        "state":"detached","observation_state":"unknown"}),
                    now + 2,
                )
                .unwrap();
            assert_eq!(
                store.get_command(&attach_id).unwrap().unwrap().state,
                CommandState::Unknown
            );
        }
        let drained = router
            .shutdown(
                &json!({"id":ulid::Ulid::new().to_string(), "mode":"drain"}),
                &Principal::Owner,
            )
            .await
            .unwrap();
        assert_eq!(drained["state"], json!("applied"));
        assert_eq!(drained["blockers"], json!([]));
        let drain = drained["drain"].as_object().expect("drain report");
        let retained = drain["retained_unresolved"]
            .as_array()
            .expect("retained unknowns");
        assert_eq!(retained.len(), 1);
        assert_eq!(retained[0]["id"], json!(attach_id));
        assert_eq!(retained[0]["verb"], json!("attach"));
        assert_eq!(retained[0]["agent"], json!(agent));
        assert_eq!(retained[0]["generation"], json!(1));
        assert_eq!(retained[0]["reason"], json!("observer_detach_fence"));
        assert_eq!(retained[0]["resolvable"], json!(false));
        assert_eq!(drain["retained_unresolved_count"], json!(1));
        assert_eq!(drain["retained_unresolved_truncated"], json!(false));
        // Unknown stays unknown: drain retires the wait, never the outcome.
        let store = router.inner.store.lock().await;
        assert_eq!(
            store.get_command(&attach_id).unwrap().unwrap().state,
            CommandState::Unknown
        );
        // Status carries the same receipt plus the retention view.
        drop(store);
        let status = router.status().await.unwrap();
        assert_eq!(status["blockers"], json!([]));
        assert_eq!(
            status["drain"]["retained_unresolved"][0]["id"],
            json!(attach_id)
        );
        let retention = status["retention"].as_object().expect("retention view");
        assert_eq!(retention["unbounded"], json!(true));
        assert_eq!(retention["db_bytes"], json!(0));
        assert_eq!(retention["wal_bytes"], json!(0));
        assert_eq!(retention["size_advisory"], Value::Null);
        assert!(retention.get("config_error").is_none());
    }

    #[test]
    fn unreadable_retention_config_is_reported_never_unbounded() {
        let policy = SecretPolicy::new(["vendor-secret"]);
        // Over the advisory size with no readable windows: a failed read
        // must not turn into "unbounded" or its advisory.
        let view = retention_view_of(
            Err(Error::new(
                "profile_error",
                "config.toml could not be parsed near vendor-secret",
            )),
            3,
            crate::config::RETENTION_UNBOUNDED_SIZE_ADVISORY_BYTES + 1,
            0,
            &policy,
        );
        view.validate().unwrap();
        assert!(!view.unbounded);
        assert_eq!(view.size_advisory, None);
        assert_eq!(
            (view.events_days, view.commands_days, view.mail_days),
            (None, None, None)
        );
        let error = view.config_error.expect("typed config error");
        assert!(error.starts_with("profile_error: config.toml could not be parsed"));
        assert!(!error.contains("vendor-secret"));
        // A readable empty policy is still the documented unbounded case.
        let unset = retention_view_of(
            Ok(crate::config::RetentionConfig::default()),
            3,
            crate::config::RETENTION_UNBOUNDED_SIZE_ADVISORY_BYTES + 1,
            0,
            &policy,
        );
        unset.validate().unwrap();
        assert!(unset.unbounded && unset.size_advisory.is_some() && unset.config_error.is_none());
    }

    /// Drive the observer detach-fence race at the Store level: attach, then
    /// detach the same subject before the reader can settle it. Returns
    /// `(agent, attach_id, detach_id)` with the attach stored `unknown`
    /// under `observer_detach_fence`. Callers model reader teardown with
    /// [`close_and_settle_observer_detach`]; without it the live detach
    /// transition still blocks drain.
    fn fenced_observer_attach(
        store: &mut eliot_store::Store,
        scope: &str,
        native_ref: &str,
        now: i64,
    ) -> (String, String, String) {
        let admitted = store
            .submit(
                "owner",
                SubmitCommand {
                    id: ulid::Ulid::new().to_string(),
                    target: None,
                    selector: None,
                    verb: "attach".to_owned(),
                    args: json!({"mode":"observe","profile":"fixture","harness":"codex",
                        "native_scope":scope,"native_ref":native_ref,
                        "profile_snapshot":{"harness":"codex"}}),
                    expected_generation: None,
                    expected_turn_id: None,
                    request_id: None,
                    probe: false,
                },
                now,
            )
            .expect("admit observer attach");
        let agent = admitted.command.target.expect("attach target");
        let attach_id = admitted.command.id.clone();
        store.dispatch(&attach_id, now).expect("dispatch attach");
        let detach_id = ulid::Ulid::new().to_string();
        store
            .submit(
                "owner",
                SubmitCommand {
                    id: detach_id.clone(),
                    target: Some(agent.clone()),
                    selector: None,
                    verb: "detach".to_owned(),
                    args: json!({"mode":"observe"}),
                    expected_generation: Some(1),
                    expected_turn_id: None,
                    request_id: None,
                    probe: false,
                },
                now + 1,
            )
            .expect("admit observer detach");
        store
            .dispatch(&detach_id, now + 1)
            .expect("dispatch detach");
        assert_eq!(
            store.get_command(&attach_id).unwrap().unwrap().state,
            CommandState::Unknown
        );
        (agent, attach_id, detach_id)
    }

    /// Model confirmed reader teardown for a fenced observer generation:
    /// close the retired observation source, then settle the detach with the
    /// exact reader-close readback the production path commits. The fenced
    /// attach stays stored `unknown`.
    fn close_and_settle_observer_detach(
        store: &mut eliot_store::Store,
        agent: &str,
        detach_id: &str,
        scope: &str,
        native_ref: &str,
        now: i64,
    ) {
        store
            .close_native_sources(agent, 1, now)
            .expect("close retired observation");
        store
            .settle_observe_detach(
                detach_id,
                json!({"agent_id":agent,"generation":2,"command_id":detach_id,
                    "native_scope":scope,"native_ref":native_ref,
                    "state":"detached","observation_state":"unknown"}),
                now + 1,
            )
            .expect("settle observer detach");
    }

    #[tokio::test]
    async fn drain_completes_with_retained_controlled_unknown_and_reconciles_unsettled() {
        let router = router();
        let now = now_ms();
        // A controlled launch goes dispatched, then a restart marks it
        // unknown and fences its generation as lost: no live process, turn
        // or reader backs it, and no owner resolution short of the receipt
        // can retire it.
        let launch_id = {
            let mut store = router.inner.store.lock().await;
            let admitted = store
                .submit(
                    "owner",
                    SubmitCommand {
                        id: ulid::Ulid::new().to_string(),
                        target: None,
                        selector: None,
                        verb: "launch".to_owned(),
                        args: json!({"project":"retained-controlled","workspace":"synthetic-workspace",
                            "harness":"codex","profile":"fake","profile_snapshot":{}}),
                        expected_generation: Some(0),
                        expected_turn_id: None,
                        request_id: None,
                        probe: false,
                    },
                    now,
                )
                .expect("admit launch");
            let agent = admitted.command.target.expect("launch target");
            let launch_id = admitted.command.id.clone();
            store.dispatch(&launch_id, now).expect("dispatch launch");
            store.recover_on_restart(now + 1).expect("restart recovery");
            assert_eq!(
                store.get_command(&launch_id).unwrap().unwrap().state,
                CommandState::Unknown
            );
            assert_eq!(
                store.get_agent(&agent).unwrap().unwrap().state,
                AgentState::Lost
            );
            launch_id
        };
        let drained = router
            .shutdown(
                &json!({"id":ulid::Ulid::new().to_string(), "mode":"drain"}),
                &Principal::Owner,
            )
            .await
            .unwrap();
        assert_eq!(drained["state"], json!("applied"));
        assert_eq!(drained["blockers"], json!([]));
        let drain = drained["drain"].as_object().expect("drain report");
        // The retained id leaves the unsettled section and its count: the
        // receipt names it once, under retained-unresolved.
        let unsettled = drain["unsettled_commands"]
            .as_array()
            .expect("unsettled commands");
        assert!(
            !unsettled
                .iter()
                .any(|command| command["id"] == json!(launch_id))
        );
        assert_eq!(drain["unsettled_commands_count"], json!(unsettled.len()));
        let retained = drain["retained_unresolved"]
            .as_array()
            .expect("retained unknowns");
        assert_eq!(retained.len(), 1);
        assert_eq!(retained[0]["id"], json!(launch_id));
        assert_eq!(retained[0]["verb"], json!("launch"));
        assert_eq!(retained[0]["resolvable"], json!(true));
        assert_eq!(retained[0]["reason"], json!("daemon_restart"));
        // Unknown stays unknown: drain retires the wait, never the outcome.
        let store = router.inner.store.lock().await;
        assert_eq!(
            store.get_command(&launch_id).unwrap().unwrap().state,
            CommandState::Unknown
        );
    }

    #[tokio::test]
    async fn retained_unknowns_survive_restart_without_memory_cache() {
        let root = std::env::temp_dir().join(format!("eliot-drain-restart-{}", ulid::Ulid::new()));
        let _temp_6 = crate::test_support::TempDir::own(root.clone());
        std::fs::create_dir(&root).unwrap();
        let db = root.join("state.sqlite");
        let (agent, attach_id) = {
            let mut store = eliot_store::Store::open(&db).unwrap();
            let (agent, attach_id, detach_id) =
                fenced_observer_attach(&mut store, "restart-scope", "restart-native", now_ms());
            close_and_settle_observer_detach(
                &mut store,
                &agent,
                &detach_id,
                "restart-scope",
                "restart-native",
                now_ms(),
            );
            assert_eq!(
                store.get_command(&attach_id).unwrap().unwrap().state,
                CommandState::Unknown
            );
            (agent, attach_id)
        };
        // Restart: a fresh Router over the same file with an empty unknown
        // cache must still classify the durable unknown from the Store page.
        let store = eliot_store::Store::open(&db).unwrap();
        let router = Router::new(
            root.join("reopened"),
            "owner-credential".to_owned(),
            Arc::new(AsyncMutex::new(store)),
            Bus::new(16),
        );
        assert!(router.unknown_ids().is_empty());
        let drained = router
            .shutdown(
                &json!({"id":ulid::Ulid::new().to_string(), "mode":"drain"}),
                &Principal::Owner,
            )
            .await
            .unwrap();
        assert_eq!(drained["state"], json!("applied"));
        assert_eq!(drained["blockers"], json!([]));
        let drain = drained["drain"].as_object().expect("drain report");
        let retained = drain["retained_unresolved"]
            .as_array()
            .expect("retained unknowns");
        assert_eq!(retained.len(), 1);
        assert_eq!(retained[0]["id"], json!(attach_id));
        assert_eq!(retained[0]["agent"], json!(agent));
        assert_eq!(retained[0]["resolvable"], json!(false));
    }

    #[tokio::test]
    async fn drain_completes_with_more_retained_unknowns_than_overflow_limit() {
        let router = router();
        let now = now_ms();
        let mut attach_ids = Vec::new();
        {
            let mut store = router.inner.store.lock().await;
            for i in 0..(STATUS_BLOCKER_MAX + 1) {
                let scope = format!("overflow-scope-{i}");
                let native_ref = format!("overflow-native-{i}");
                let (agent, attach_id, detach_id) =
                    fenced_observer_attach(&mut store, &scope, &native_ref, now);
                close_and_settle_observer_detach(
                    &mut store,
                    &agent,
                    &detach_id,
                    &scope,
                    &native_ref,
                    now + 2,
                );
                attach_ids.push(attach_id);
            }
        }
        // Populate the memory cache the way command.unknown events would in
        // production: pre-fix code turns these 65 into 64 blockers plus a
        // permanent overflow blocker.
        for attach_id in &attach_ids {
            router
                .note_unknown_if_uncertain(attach_id, "observer_detach_fence")
                .await;
        }
        assert_eq!(router.unknown_ids().len(), STATUS_BLOCKER_MAX + 1);
        let drained = router
            .shutdown(
                &json!({"id":ulid::Ulid::new().to_string(), "mode":"drain"}),
                &Principal::Owner,
            )
            .await
            .unwrap();
        assert_eq!(drained["state"], json!("applied"));
        assert_eq!(drained["blockers"], json!([]));
        let drain = drained["drain"].as_object().expect("drain report");
        // The unknowns are still reported honestly as unknown; they just no
        // longer block, and the overflow counter never fires for them.
        assert_eq!(
            drain["unknown_commands_count"],
            json!(STATUS_BLOCKER_MAX + 1)
        );
        assert_eq!(
            drain["retained_unresolved_count"],
            json!(STATUS_BLOCKER_MAX + 1)
        );
        assert_eq!(
            drain["retained_unresolved"]
                .as_array()
                .expect("retained unknowns")
                .len(),
            STATUS_BLOCKER_MAX
        );
        assert_eq!(drain["retained_unresolved_truncated"], json!(true));
    }

    #[tokio::test]
    async fn drain_blockers_keep_live_command_visible_behind_retained_prefix() {
        let router = router();
        let now = now_ms();
        // Fill the whole bounded unsettled prefix with retained controlled
        // unknowns: dispatched launches fenced as lost by a restart, with
        // no live process, turn or reader behind them.
        let mut retained_ids = Vec::new();
        let live_id = {
            let mut store = router.inner.store.lock().await;
            for i in 0..STATUS_BLOCKER_MAX {
                let admitted = store
                    .submit(
                        "owner",
                        SubmitCommand {
                            id: ulid::Ulid::new().to_string(),
                            target: None,
                            selector: None,
                            verb: "launch".to_owned(),
                            args: json!({"project":"prefix-scope","workspace":"synthetic-workspace",
                                "harness":"codex","profile":"synthetic","profile_snapshot":{}}),
                            expected_generation: Some(0),
                            expected_turn_id: None,
                            request_id: None,
                            probe: false,
                        },
                        now + i as i64,
                    )
                    .expect("admit prefix launch");
                retained_ids.push(admitted.command.id.clone());
                store
                    .dispatch(&retained_ids[i], now + i as i64)
                    .expect("dispatch prefix launch");
            }
            store
                .recover_on_restart(now + 10_000)
                .expect("restart recovery");
            for id in &retained_ids {
                assert_eq!(
                    store.get_command(id).unwrap().unwrap().state,
                    CommandState::Unknown
                );
            }
            // A later live launch sorts behind the whole retained prefix and
            // stays dispatched: it must still block even though the retained
            // page used to cover the bound.
            let live = store
                .submit(
                    "owner",
                    SubmitCommand {
                        id: ulid::Ulid::new().to_string(),
                        target: None,
                        selector: None,
                        verb: "launch".to_owned(),
                        args: json!({"project":"prefix-live","workspace":"synthetic-workspace",
                            "harness":"codex","profile":"synthetic","profile_snapshot":{}}),
                        expected_generation: Some(0),
                        expected_turn_id: None,
                        request_id: None,
                        probe: false,
                    },
                    now + 20_000,
                )
                .expect("admit live launch")
                .command
                .id;
            store
                .dispatch(&live, now + 20_001)
                .expect("dispatch live launch");
            live
        };
        // The retained ids retire from the unsettled blockers, but the live
        // command they used to hide behind the prefix must block.
        let blockers = router.status_blockers().await.expect("blockers");
        assert!(
            blockers.iter().any(|blocker| matches!(
                blocker,
                StatusBlocker::CommandUnsettled { command_id, state }
                    if command_id == &live_id && *state == CommandState::Dispatched
            )),
            "live command hidden behind retained prefix: {blockers:?}"
        );
        assert!(
            !blockers.iter().any(|blocker| matches!(
                blocker,
                StatusBlocker::CommandUnsettled { command_id, .. }
                    if retained_ids.contains(command_id)
            )),
            "retained unknowns must not block: {blockers:?}"
        );
        let drained = router
            .shutdown(
                &json!({"id":ulid::Ulid::new().to_string(), "mode":"drain"}),
                &Principal::Owner,
            )
            .await
            .unwrap();
        assert_eq!(drained["state"], json!("dispatched"));
        let drain = drained["drain"].as_object().expect("drain report");
        let unsettled = drain["unsettled_commands"]
            .as_array()
            .expect("unsettled commands");
        assert!(
            unsettled
                .iter()
                .any(|command| command["id"] == json!(live_id)),
            "live command missing from drain receipt"
        );
        assert_eq!(drain["unsettled_commands_count"], json!(unsettled.len()));
        assert_eq!(
            drain["retained_unresolved"]
                .as_array()
                .expect("retained unknowns")
                .len(),
            STATUS_BLOCKER_MAX
        );
        assert_eq!(drain["retained_unresolved_truncated"], json!(false));
        // The live command still gates the exit: drain must not end early.
        router.response_finished(true);
        assert!(!router.should_exit().await.expect("should_exit"));
    }

    #[tokio::test]
    async fn drain_blocks_live_command_hidden_behind_fully_retained_prefix() {
        let router = router();
        let now = now_ms();
        // Fill the whole bounded unsettled prefix with retained controlled
        // unknowns, exactly like the prefix test above, but the live command
        // behind them is an observer attach: its Agent is observe-only, so
        // no AgentLive blocker can mask a hidden command. Without the SQL
        // exclusion this drain read back as applied (early exit) because
        // the retained prefix occupied the whole bounded page.
        let mut retained_ids = Vec::new();
        {
            let mut store = router.inner.store.lock().await;
            for i in 0..STATUS_BLOCKER_MAX {
                let admitted = store
                    .submit(
                        "owner",
                        SubmitCommand {
                            id: ulid::Ulid::new().to_string(),
                            target: None,
                            selector: None,
                            verb: "launch".to_owned(),
                            args: json!({"project":"prefix-scope","workspace":"synthetic-workspace",
                                "harness":"codex","profile":"synthetic","profile_snapshot":{}}),
                            expected_generation: Some(0),
                            expected_turn_id: None,
                            request_id: None,
                            probe: false,
                        },
                        now + i as i64,
                    )
                    .expect("admit prefix launch");
                retained_ids.push(admitted.command.id.clone());
                store
                    .dispatch(&retained_ids[i], now + i as i64)
                    .expect("dispatch prefix launch");
            }
            store
                .recover_on_restart(now + 10_000)
                .expect("restart recovery");
            for id in &retained_ids {
                assert_eq!(
                    store.get_command(id).unwrap().unwrap().state,
                    CommandState::Unknown
                );
            }
        }
        // A live observer attach sorts behind the retained prefix and stays
        // dispatched: observe-only Agents never list as AgentLive, so only
        // the unsettled section can keep this command blocking.
        let live_id = {
            let mut store = router.inner.store.lock().await;
            let live = store
                .submit(
                    "owner",
                    SubmitCommand {
                        id: ulid::Ulid::new().to_string(),
                        target: None,
                        selector: None,
                        verb: "attach".to_owned(),
                        args: json!({"mode":"observe","profile":"fixture","harness":"codex",
                            "native_scope":"prefix-live","native_ref":"prefix-live",
                            "profile_snapshot":{"harness":"codex"}}),
                        expected_generation: None,
                        expected_turn_id: None,
                        request_id: None,
                        probe: false,
                    },
                    now + 20_000,
                )
                .expect("admit live attach")
                .command
                .id;
            store
                .dispatch(&live, now + 20_001)
                .expect("dispatch live attach");
            assert_eq!(
                store.get_command(&live).unwrap().unwrap().state,
                CommandState::Dispatched
            );
            live
        };
        let blockers = router.status_blockers().await.expect("blockers");
        assert!(
            blockers.iter().any(|blocker| matches!(
                blocker,
                StatusBlocker::CommandUnsettled { command_id, state }
                    if command_id == &live_id && *state == CommandState::Dispatched
            )),
            "live command hidden behind retained prefix: {blockers:?}"
        );
        assert!(
            !blockers.iter().any(|blocker| matches!(
                blocker,
                StatusBlocker::CommandUnsettled { command_id, .. }
                    if retained_ids.contains(command_id)
            )),
            "retained unknowns must not block: {blockers:?}"
        );
        assert!(
            !blockers
                .iter()
                .any(|blocker| matches!(blocker, StatusBlocker::CommandUnsettledMore { .. })),
            "one listed live command needs no overflow remainder: {blockers:?}"
        );
        let drained = router
            .shutdown(
                &json!({"id":ulid::Ulid::new().to_string(), "mode":"drain"}),
                &Principal::Owner,
            )
            .await
            .unwrap();
        assert_eq!(drained["state"], json!("dispatched"));
        let drain = drained["drain"].as_object().expect("drain report");
        let unsettled = drain["unsettled_commands"]
            .as_array()
            .expect("unsettled commands");
        assert_eq!(unsettled.len(), 1);
        assert_eq!(unsettled[0]["id"], json!(live_id));
        assert_eq!(drain["unsettled_commands_count"], json!(1));
        assert_eq!(drain["unsettled_commands_truncated"], json!(false));
        assert_eq!(
            drain["retained_unresolved_count"],
            json!(STATUS_BLOCKER_MAX)
        );
        assert_eq!(drain["retained_unresolved_truncated"], json!(false));
        // The live command still gates the exit: drain must not end early.
        router.response_finished(true);
        assert!(!router.should_exit().await.expect("should_exit"));
    }

    #[tokio::test]
    async fn drain_blocks_for_active_run_worker_past_row_settlement() {
        let router = router();
        let now = now_ms();
        // A dispatched launch whose row settles while its script worker is
        // still registered: the worker outlives the row it used to block on.
        let run_id = {
            let mut store = router.inner.store.lock().await;
            let admitted = store
                .submit(
                    "owner",
                    SubmitCommand {
                        id: ulid::Ulid::new().to_string(),
                        target: None,
                        selector: None,
                        verb: "launch".to_owned(),
                        args: json!({"project":"run-worker","workspace":"synthetic-workspace",
                            "harness":"codex","profile":"synthetic","profile_snapshot":{}}),
                        expected_generation: Some(0),
                        expected_turn_id: None,
                        request_id: None,
                        probe: false,
                    },
                    now,
                )
                .expect("admit run command")
                .command
                .id;
            store
                .dispatch(&admitted, now)
                .expect("dispatch run command");
            admitted
        };
        // The registered worker stays unfinished until released, mirroring a
        // script child that is still settling its output admission.
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let (completed_tx, completed_rx) = tokio::sync::watch::channel(false);
        let worker_router = std::sync::Arc::downgrade(&router.inner);
        let worker_id = run_id.clone();
        let task = tokio::spawn(async move {
            let mut completion = super::super::command::RunWorkerCompletion {
                router: worker_router,
                run_id: worker_id,
                completed: completed_tx,
                finished: false,
            };
            let _ = release_rx.await;
            completion.finish();
        });
        router.inner.run_workers.lock_recover().insert(
            run_id.clone(),
            super::super::RunWorkerHandle {
                task,
                completed: completed_rx,
            },
        );
        // Settle the row while the worker is still active: the worker must
        // keep blocking under its own entry, without duplicating the row.
        {
            let mut store = router.inner.store.lock().await;
            store
                .settle(
                    &run_id,
                    CommandState::Failed,
                    Some(json!({"code":"synthetic"})),
                    now + 1,
                )
                .expect("settle run row");
        }
        let blockers = router.status_blockers().await.expect("blockers");
        assert!(
            blockers.iter().any(|blocker| matches!(
                blocker,
                StatusBlocker::ScriptRunActive { run_id: active } if active == &run_id
            )),
            "settled-row run worker must block explicitly: {blockers:?}"
        );
        assert!(
            !blockers.iter().any(|blocker| matches!(
                blocker,
                StatusBlocker::CommandUnsettled { command_id, .. } if command_id == &run_id
            )),
            "settled row must not double-list: {blockers:?}"
        );
        let drained = router
            .shutdown(
                &json!({"id":ulid::Ulid::new().to_string(), "mode":"drain"}),
                &Principal::Owner,
            )
            .await
            .unwrap();
        assert_eq!(drained["state"], json!("dispatched"));
        router.response_finished(true);
        assert!(!router.should_exit().await.expect("should_exit"));
        // Releasing the worker retires the gate without touching the row.
        release_tx.send(()).expect("release run worker");
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            router.wait_run_worker(&run_id),
        )
        .await
        .expect("worker restraint");
        assert!(router.inner.run_workers.lock_recover().is_empty());
        assert!(!router.run_workers_active());
    }

    #[tokio::test]
    async fn drain_overflow_fallback_counts_live_commands_beyond_bound() {
        let router = router();
        let now = now_ms();
        // More live unsettled commands than the bounded list holds: every
        // command stays live, so the unlisted remainder must keep blocking
        // with its exact count instead of reading back as drained.
        {
            let mut store = router.inner.store.lock().await;
            for i in 0..=(STATUS_BLOCKER_MAX as i64) {
                let admitted = store
                    .submit(
                        "owner",
                        SubmitCommand {
                            id: ulid::Ulid::new().to_string(),
                            target: None,
                            selector: None,
                            verb: "launch".to_owned(),
                            args: json!({"project":"overflow-live","workspace":"synthetic-workspace",
                                "harness":"codex","profile":"synthetic","profile_snapshot":{}}),
                            expected_generation: Some(0),
                            expected_turn_id: None,
                            request_id: None,
                            probe: false,
                        },
                        now + i,
                    )
                    .expect("admit overflow launch");
                store
                    .dispatch(&admitted.command.id, now + i)
                    .expect("dispatch overflow launch");
            }
        }
        let blockers = router.status_blockers().await.expect("blockers");
        let listed = blockers
            .iter()
            .filter(|blocker| matches!(blocker, StatusBlocker::CommandUnsettled { .. }))
            .count();
        assert_eq!(listed, STATUS_BLOCKER_MAX);
        assert!(
            blockers.iter().any(|blocker| matches!(
                blocker,
                StatusBlocker::CommandUnsettledMore { count } if *count == 1
            )),
            "unlisted live command must block with its remainder: {blockers:?}"
        );
        let drained = router
            .shutdown(
                &json!({"id":ulid::Ulid::new().to_string(), "mode":"drain"}),
                &Principal::Owner,
            )
            .await
            .unwrap();
        assert_eq!(drained["state"], json!("dispatched"));
        router.response_finished(true);
        assert!(!router.should_exit().await.expect("should_exit"));
    }

    #[tokio::test]
    async fn drain_report_uses_store_custody_not_routing_or_operation_locks() {
        let router = router();
        // One observed native child, one observed workspace child, one
        // routing-only entry with no Store custody, and one bare workspace
        // preparation with no observed child.
        let launch = |store: &mut eliot_store::Store, project: &str| {
            let launch_id = ulid::Ulid::new().to_string();
            let agent = store
                .submit(
                    "owner",
                    SubmitCommand {
                        id: launch_id.clone(),
                        target: None,
                        selector: None,
                        verb: "launch".to_owned(),
                        args: json!({
                            "project":project, "workspace":"synthetic-workspace",
                            "harness":"codex", "profile":"synthetic", "profile_snapshot":{},
                        }),
                        expected_generation: Some(0),
                        expected_turn_id: None,
                        request_id: None,
                        probe: false,
                    },
                    now_ms(),
                )
                .unwrap()
                .command
                .target
                .unwrap();
            store.dispatch(&launch_id, now_ms()).unwrap();
            agent
        };
        let prepare_workspace = |store: &mut eliot_store::Store, project: &str| {
            let branch = format!("codex/drain-custody-{project}");
            let worktree = format!("/synthetic-worktree-{project}");
            store
                .submit_with_workspace(
                    "owner",
                    SubmitCommand {
                        id: ulid::Ulid::new().to_string(),
                        target: None,
                        selector: None,
                        verb: "launch".to_owned(),
                        args: json!({"project":project, "workspace":"/synthetic-source", "worktree":branch, "harness":"codex", "profile":"fixture", "profile_snapshot":{}}),
                        expected_generation: Some(0),
                        expected_turn_id: None,
                        request_id: None,
                        probe: false,
                    },
                    WorkspacePreparationInput {
                        project: project.to_owned(),
                        repo_path: "/synthetic-source".to_owned(),
                        repo_common_dir: "/synthetic-source/.git".to_owned(),
                        requested_branch: branch,
                        base_ref: None,
                        base_oid: "b".repeat(40),
                        worktree_path: worktree,
                        frozen_setup: Vec::new(),
                        generated: None,
                    },
                    now_ms(),
                )
                .unwrap()
                .command
                .target
                .unwrap()
        };
        let (plugin_agent, workspace_agent, routing_only, bare_preparation) = {
            let mut store = router.inner.store.lock().await;
            let plugin_agent = launch(&mut store, "drain-custody-plugin");
            store
                .ensure_process_source(&plugin_agent, 1, now_ms())
                .unwrap();
            store
                .observe_processes(
                    &plugin_agent,
                    1,
                    &[eliot_core::ProcessRecord {
                        identity: eliot_core::ProcessIdentity {
                            host_boot_id: "drain-boot".to_owned(),
                            pid: 7,
                            start_time: 1,
                        },
                        owner: format!("agent:{plugin_agent}#1"),
                        role: "harness".to_owned(),
                        state: eliot_core::ProcessState::Running,
                        exit_code: None,
                        updated_at: now_ms(),
                    }],
                    now_ms(),
                )
                .unwrap();
            let workspace_agent = prepare_workspace(&mut store, "drain-custody-ws");
            store
                .ensure_workspace_process_source(&workspace_agent, 1, now_ms())
                .unwrap();
            store
                .observe_workspace_processes(
                    &workspace_agent,
                    1,
                    &eliot_core::ProcessSnapshot {
                        samples: vec![eliot_core::ProcessSample {
                            record: eliot_core::ProcessRecord {
                                identity: eliot_core::ProcessIdentity {
                                    host_boot_id: "drain-boot".to_owned(),
                                    pid: 8,
                                    start_time: 1,
                                },
                                owner: format!("workspace:{workspace_agent}#1"),
                                role: "setup".to_owned(),
                                state: eliot_core::ProcessState::Running,
                                exit_code: None,
                                updated_at: now_ms(),
                            },
                            cpu_time_ns: None,
                            rss_bytes: None,
                        }],
                        complete: true,
                    },
                    now_ms(),
                )
                .unwrap();
            let routing_only = launch(&mut store, "drain-custody-routing");
            let bare_preparation = prepare_workspace(&mut store, "drain-custody-bare");
            (
                plugin_agent,
                workspace_agent,
                routing_only,
                bare_preparation,
            )
        };
        // Routing eligibility alone is not custody: no Store process record
        // backs this generation.
        router
            .inner
            .plugin_host
            .mark_generation_controlled_for_test(&routing_only, 1);
        let shutdown = router
            .shutdown(
                &json!({"id":ulid::Ulid::new().to_string(), "mode":"drain"}),
                &Principal::Owner,
            )
            .await
            .unwrap();
        let drain = shutdown["drain"].as_object().expect("drain report");
        let owned = drain["owned_processes"]
            .as_array()
            .expect("owned processes");
        assert_eq!(owned.len(), 2);
        assert!(
            owned
                .iter()
                .any(|process| process["agent"] == json!(plugin_agent)
                    && process["generation"] == json!(1)
                    && process["kind"] == json!("plugin"))
        );
        assert!(
            owned
                .iter()
                .any(|process| process["agent"] == json!(workspace_agent)
                    && process["generation"] == json!(1)
                    && process["kind"] == json!("workspace"))
        );
        assert!(
            !owned
                .iter()
                .any(|process| process["agent"] == json!(routing_only)),
            "routing-only generation must not read as custody"
        );
        assert!(
            !owned
                .iter()
                .any(|process| process["agent"] == json!(bare_preparation)),
            "bare workspace preparation must not read as a live process"
        );
        assert_eq!(drain["owned_processes_count"], json!(2));
        assert_eq!(drain["owned_processes_truncated"], json!(false));
    }

    #[tokio::test]
    async fn drain_report_truncation_covers_list_bound() {
        let router = router();
        {
            let mut store = router.inner.store.lock().await;
            for _ in 0..70 {
                store
                    .submit(
                        "owner",
                        SubmitCommand {
                            id: ulid::Ulid::new().to_string(),
                            target: None,
                            selector: None,
                            verb: "launch".to_owned(),
                            args: json!({
                                "project":"drain-truncation", "workspace":"synthetic-workspace",
                                "harness":"codex", "profile":"synthetic", "profile_snapshot":{},
                            }),
                            expected_generation: Some(0),
                            expected_turn_id: None,
                            request_id: None,
                            probe: false,
                        },
                        now_ms(),
                    )
                    .unwrap();
            }
        }
        let shutdown = router
            .shutdown(
                &json!({"id":ulid::Ulid::new().to_string(), "mode":"drain"}),
                &Principal::Owner,
            )
            .await
            .unwrap();
        let drain = shutdown["drain"].as_object().expect("drain report");
        let unsettled = drain["unsettled_commands"]
            .as_array()
            .expect("unsettled commands");
        assert_eq!(unsettled.len(), 64);
        assert_eq!(drain["unsettled_commands_count"], json!(64));
        assert_eq!(drain["unsettled_commands_truncated"], json!(true));
        // No Agent has an active turn, but the Agent page itself is capped,
        // so a hidden turn can never read back as complete.
        assert_eq!(drain["running_turns"], json!([]));
        assert_eq!(drain["running_turns_count"], json!(0));
        assert_eq!(drain["running_turns_truncated"], json!(true));
    }

    #[tokio::test]
    async fn drain_report_truncation_covers_byte_trimmed_pages() {
        let router = router();
        {
            let mut store = router.inner.store.lock().await;
            let (agent, _) = result_fixture(&mut store);
            let big = "x".repeat(100_000);
            for _ in 0..12 {
                store
                    .submit(
                        "owner",
                        SubmitCommand {
                            id: ulid::Ulid::new().to_string(),
                            target: Some(agent.clone()),
                            selector: None,
                            verb: "send".to_owned(),
                            args: json!({"text": big}),
                            expected_generation: Some(1),
                            expected_turn_id: None,
                            request_id: None,
                            probe: false,
                        },
                        now_ms(),
                    )
                    .unwrap();
            }
        }
        let shutdown = router
            .shutdown(
                &json!({"id":ulid::Ulid::new().to_string(), "mode":"drain"}),
                &Principal::Owner,
            )
            .await
            .unwrap();
        let drain = shutdown["drain"].as_object().expect("drain report");
        let unsettled = drain["unsettled_commands"]
            .as_array()
            .expect("unsettled commands");
        // Twelve ~100 KiB sends plus the fixture send overflow the ~900 KiB
        // page, so the bounded prefix omits qualifying commands.
        assert!(
            unsettled.len() < 13,
            "byte-trimmed page kept {}",
            unsettled.len()
        );
        assert_eq!(drain["unsettled_commands_count"], json!(unsettled.len()));
        assert_eq!(drain["unsettled_commands_truncated"], json!(true));
        // The single Agent page is complete, so its turn flag stays false.
        assert_eq!(drain["running_turns_truncated"], json!(false));
    }

    #[tokio::test]
    async fn typed_agent_views_status_and_shutdown_keep_identical_blocker_wire() {
        let router = router();
        {
            let mut store = router.inner.store.lock().await;
            admit(&mut store);
        }
        let status = router
            .handle(
                "daemon.status",
                json!({
                    "_auth":{"kind":"owner", "credential":"owner-credential"}, "_payload":{}
                }),
            )
            .await
            .unwrap();
        let typed: DaemonStatusResponse = serde_json::from_value(status.clone()).unwrap();
        assert_eq!(typed.state, "running");
        assert_eq!(typed.admission, "open");
        assert_eq!(typed.agents, 1);
        assert!(
            typed
                .blockers
                .iter()
                .any(|blocker| matches!(blocker, StatusBlocker::AgentLive { .. }))
        );
        assert!(
            typed
                .blockers
                .iter()
                .any(|blocker| matches!(blocker, StatusBlocker::CommandUnsettled { .. }))
        );
        let shutdown = router
            .shutdown(
                &json!({"id":ulid::Ulid::new().to_string(), "mode":"drain"}),
                &Principal::Owner,
            )
            .await
            .unwrap();
        assert_eq!(shutdown["blockers"], status["blockers"]);
        assert_eq!(shutdown["state"], json!("dispatched"));
        assert_eq!(shutdown["mode"], json!("drain"));
        assert_eq!(
            router.status().await.unwrap()["admission"],
            json!("draining")
        );
        assert!(!router.should_exit().await.unwrap());
    }
}
