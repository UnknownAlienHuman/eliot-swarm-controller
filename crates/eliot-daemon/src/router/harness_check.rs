//! Swarm's own post-update harness check (#932 item 3).
//!
//! When the installed version of a harness changes, the daemon runs one short
//! scenario for that harness and version on a daemon-owned isolated Agent: a
//! root chat with no project, carrying the check's harness and installed
//! version in its frozen snapshot. The scenario runs launch, a turn with a
//! tool call, steer, stop and resume on the cheapest configured model for the
//! harness, inside a hard wall-clock budget, and never touches production
//! Agents, worktrees or the Owner's harness sessions.
//!
//! A new installed version is always accepted and marked "not checked"; it is
//! never refused. At most one recorded run exists per harness and version. On
//! failure, automatic routing stops choosing that harness until a later check
//! passes; explicit Owner launches bypass routing and still work. The result
//! goes to the Owner inbox through the `harness_check` projection.

use super::*;
use tokio::sync::watch;

/// Scenario-drive result. `CheckEnd` is declared below; the alias keeps
/// drive signatures exact while the shared `Result` alias stays single-param.
type CheckResult<T> = std::result::Result<T, CheckEnd>;

/// One admitted check scenario: the claim is held in Store until the drive
/// records its outcome.
pub(crate) struct HarnessCheckWork {
    agent_id: String,
    harness: String,
    version: String,
    profile: String,
    launch_command_id: String,
    send_command_id: String,
    deadline: Instant,
}

/// Bounded stop/cleanup wait inside a check drive.
const CHECK_CLEANUP_WAIT: Duration = Duration::from_secs(5);
/// How long the drive waits for the scenario turn to start before steering.
const CHECK_STEER_ARM_WAIT: Duration = Duration::from_secs(30);

impl Router {
    /// Note current installations and admit at most `available` due checks.
    /// Store admission claims each version before launch dispatch can spawn a
    /// native process, so concurrent ticks and restarts never run one version
    /// twice. Orphaned check Agents from a dead daemon are collected for
    /// deletion by the caller.
    pub(crate) async fn maintain_harness_checks(
        &self,
        available: usize,
    ) -> Result<(Vec<HarnessCheckWork>, Vec<String>)> {
        if self.inner.mode.load(Ordering::SeqCst) != MODE_OPEN {
            return Ok((Vec::new(), Vec::new()));
        }
        // Read-only installation probes run outside the Store lock.
        let mut probes = Vec::new();
        for harness in eliot_core::api::harness::SUPPORTED_HARNESSES {
            probes.push((
                *harness,
                crate::adhoc::harness_installation_async(harness).await,
            ));
        }
        let now = now_ms();
        let (due, orphans) = {
            let mut store = self.inner.store.lock().await;
            let settled = store
                .settle_stale_harness_attempts(now, eliot_core::HARNESS_CHECK_STALE_ATTEMPT_MS)?;
            if !settled.is_empty() {
                crate::logging::record(
                    &self.secret_policy_snapshot(),
                    "harness.check_claim_expired",
                    &[("harness", settled.join(",").as_str())],
                    None,
                );
            }
            for (harness, entry) in &probes {
                let version = entry
                    .get("version")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                store.note_harness_installed(harness, version.as_deref(), now)?;
            }
            // Fetch every due harness: ones without a configured profile are
            // skipped below, and must not consume the run batch.
            let due = store.due_harness_checks(now, usize::MAX)?;
            let orphans = if available > 0 {
                self.orphaned_harness_check_agents(&store)?
            } else {
                Vec::new()
            };
            (due, orphans)
        };
        let mut started = Vec::with_capacity(due.len());
        for (harness, version) in due {
            if started.len() >= available.min(eliot_core::HARNESS_CHECK_BATCH_LIMIT) {
                break;
            }
            let Some(profile) = self.cheapest_harness_profile(&harness) else {
                crate::logging::record(
                    &self.secret_policy_snapshot(),
                    "harness.check_deferred",
                    &[("harness", harness.as_str())],
                    None,
                );
                continue;
            };
            let claimed = {
                let mut store = self.inner.store.lock().await;
                store.claim_harness_check(&harness, &version, &profile, now)?
            };
            if !claimed {
                continue;
            }
            match self.start_harness_check(&harness, &version, &profile).await {
                Ok(work) => started.push(work),
                Err((error, step)) => {
                    self.defer_harness_check(&harness, &version, &profile, step, &error)
                        .await;
                }
            }
        }
        Ok((started, orphans))
    }

    /// Check Agents whose run can no longer complete: no live claim holds
    /// their harness and version, or their version already has its recorded
    /// run. Reads committed state only; the caller deletes them unlocked.
    fn orphaned_harness_check_agents(&self, store: &eliot_store::Store) -> Result<Vec<String>> {
        let mut orphans = Vec::new();
        let mut after: Option<String> = None;
        loop {
            let (page, more) = store.list_agents(after.as_deref(), 64)?;
            if page.is_empty() {
                break;
            }
            for agent in &page {
                after = Some(agent.id.clone());
                if !agent.is_harness_check() {
                    continue;
                }
                let marker = agent
                    .profile_snapshot
                    .get(eliot_core::HARNESS_CHECK_LAUNCH_ARG);
                let marker_version = marker
                    .and_then(|marker| marker.get("version"))
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let row = store.get_harness_check(&agent.harness)?;
                let current = row.as_ref().is_some_and(|row| {
                    !row.version_checked()
                        && row.installed_version.as_deref().is_some_and(|installed| {
                            eliot_core::same_harness_version(
                                &agent.harness,
                                installed,
                                marker_version,
                            )
                        })
                });
                let claimed =
                    store
                        .harness_check_attempt(&agent.harness)?
                        .is_some_and(|(attempted, _)| {
                            eliot_core::same_harness_version(
                                &agent.harness,
                                &attempted,
                                marker_version,
                            )
                        });
                if !(current && claimed) {
                    orphans.push(agent.id.clone());
                }
            }
            if !more {
                break;
            }
        }
        Ok(orphans)
    }

    /// Cheapest configured profile for one harness, preferring profiles with
    /// an explicit model. Returns `None` when no profile names the harness:
    /// the version stays "not checked" and the tick retries the local lookup.
    fn cheapest_harness_profile(&self, harness: &str) -> Option<String> {
        let mut ranked: Vec<(String, Option<String>)> =
            crate::config::profile_names_for_harness(&self.inner.data_root, harness)
                .into_iter()
                .filter_map(|name| {
                    let snapshot = self.resolve_profile_cached(&name).ok()?;
                    if snapshot.get("harness").and_then(Value::as_str)? != harness {
                        return None;
                    }
                    let model = snapshot
                        .get("model")
                        .and_then(Value::as_str)
                        .filter(|model| !model.trim().is_empty())
                        .or_else(|| {
                            snapshot
                                .get("daemon_model")
                                .and_then(Value::as_str)
                                .filter(|model| !model.trim().is_empty())
                        })
                        .map(str::to_owned);
                    Some((name, model))
                })
                .collect();
        if ranked.is_empty() {
            return None;
        }
        if ranked.iter().any(|(_, model)| model.is_some()) {
            ranked.retain(|(_, model)| model.is_some());
        }
        ranked.sort_by(|left, right| {
            eliot_core::cheapest_model_rank(left.1.as_deref().unwrap_or_default()).cmp(
                &eliot_core::cheapest_model_rank(right.1.as_deref().unwrap_or_default()),
            )
        });
        ranked.into_iter().next().map(|(name, _)| name)
    }

    /// Admit the scenario launch for an already-claimed version. Failures
    /// carry the step to record: profile selection or launch.
    async fn start_harness_check(
        &self,
        harness: &str,
        version: &str,
        profile: &str,
    ) -> std::result::Result<HarnessCheckWork, (Box<Error>, &'static str)> {
        let snapshot = self.resolve_profile_cached(profile).map_err(|error| {
            (
                Box::new(Error::new(
                    error.code,
                    format!(
                        "harness check profile {profile} stopped resolving: {}",
                        error.message
                    ),
                )),
                "select_profile",
            )
        })?;
        if snapshot.get("harness").and_then(Value::as_str) != Some(harness) {
            return Err((
                Box::new(Error::new(
                    "invalid",
                    "harness check profile changed its harness after selection",
                )),
                "select_profile",
            ));
        }
        let mut request = SubmitCommand::new(ulid::Ulid::new().to_string(), "launch");
        request.args = json!({
            "profile": profile,
            "chat": true,
            "text": eliot_core::HARNESS_CHECK_TOOL_PROMPT,
        });
        // A daemon-owned root chat with no project, registered workspace,
        // sources, or user task. The model can only affect its private chat
        // directory. Hermetic like a provider probe, except the scenario
        // needs one native tool call, so MCP stays minimal while native
        // tools stay available.
        authorize_command(&Principal::Owner, &mut request)
            .map_err(|error| (Box::new(error), "launch"))?;
        let prepared = self
            .prepare_submission_with_routing(
                request,
                &Principal::Owner,
                false,
                false,
                routing::LaunchRouting {
                    harness_check_version: Some(version),
                    ..Default::default()
                },
                true,
            )
            .await
            .map_err(|error| (Box::new(error), "launch"))?;
        let snapshot = prepared
            .request
            .args
            .get("profile_snapshot")
            .ok_or_else(|| {
                (
                    Box::new(Error::new(
                        "source_integrity",
                        "harness check launch lost its frozen profile snapshot",
                    )),
                    "launch",
                )
            })?;
        if snapshot.get("harness").and_then(Value::as_str) != Some(harness) {
            return Err((
                Box::new(Error::new(
                    "unsupported",
                    "harness check harness changed during preparation",
                )),
                "launch",
            ));
        }
        let refusal = self
            .inner
            .store
            .lock()
            .await
            .routing_refusal(profile, harness, snapshot, None)
            .map_err(|error| (Box::new(error), "launch"))?;
        if let Some(error) = refusal
            && error.code == "provider_held"
        {
            return Err((Box::new(error), "launch"));
        }
        let admission = self
            .admit_check_command(prepared)
            .await
            .map_err(|error| (Box::new(error), "launch"))?;
        let send = admission.linked_command.as_ref().ok_or_else(|| {
            (
                Box::new(Error::new(
                    "source_integrity",
                    "harness check launch did not admit its linked SEND",
                )),
                "launch",
            )
        })?;
        let agent_id = send.target.clone().ok_or_else(|| {
            (
                Box::new(Error::new(
                    "source_integrity",
                    "harness check SEND has no Agent target",
                )),
                "launch",
            )
        })?;
        Ok(HarnessCheckWork {
            agent_id,
            harness: harness.to_owned(),
            version: version.to_owned(),
            profile: profile.to_owned(),
            launch_command_id: admission.command.id,
            send_command_id: send.id.clone(),
            deadline: Instant::now() + Duration::from_secs(eliot_core::HARNESS_CHECK_TIMEOUT_SECS),
        })
    }

    /// A claimed version that could not start: quota refusals release the run
    /// for after the hold, transient states release the claim for the next
    /// tick, and every other start failure records its step with the error.
    async fn defer_harness_check(
        &self,
        harness: &str,
        version: &str,
        profile: &str,
        step: &'static str,
        error: &Error,
    ) {
        let now = now_ms();
        if eliot_core::is_quota_refusal(&error.code) {
            let mut store = self.inner.store.lock().await;
            if let Err(record_error) =
                store.record_harness_check_quota(harness, version, profile, now)
            {
                crate::logging::record(
                    &self.secret_policy_snapshot(),
                    "daemon.harness_check_quota_unrecorded",
                    &[("harness", harness)],
                    Some(&record_error),
                );
            }
            return;
        }
        // Transient dispatch states release the claim without consuming the
        // version's run, so the next tick retries the cheap local path.
        if matches!(
            error.code.as_str(),
            "busy" | "unavailable" | "capacity_unavailable"
        ) {
            let mut store = self.inner.store.lock().await;
            if let Err(release_error) = store.release_harness_check_claim(harness, version, now) {
                crate::logging::record(
                    &self.secret_policy_snapshot(),
                    "daemon.harness_check_claim_unreleased",
                    &[("harness", harness)],
                    Some(&release_error),
                );
            }
            return;
        }
        let mut store = self.inner.store.lock().await;
        if let Err(record_error) = store.record_harness_check_failed(
            harness,
            version,
            profile,
            step,
            &check_error_text(error),
            now,
        ) {
            crate::logging::record(
                &self.secret_policy_snapshot(),
                "daemon.harness_check_launch_unrecorded",
                &[("harness", harness)],
                Some(&record_error),
            );
        }
    }

    /// Drive one admitted scenario to its recorded outcome, then delete its
    /// Agent. On shutdown, restart maintenance settles any expired claim.
    pub(crate) async fn drive_harness_check(
        &self,
        work: HarnessCheckWork,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<()> {
        let result = self.run_check_scenario(&work, &mut shutdown).await;
        let now = now_ms();
        match &result {
            Ok(()) => {
                let mut store = self.inner.store.lock().await;
                if let Err(error) = store.record_harness_check_passed(
                    &work.harness,
                    &work.version,
                    &work.profile,
                    now,
                ) {
                    crate::logging::record(
                        &self.secret_policy_snapshot(),
                        "daemon.harness_check_pass_unrecorded",
                        &[("harness", work.harness.as_str())],
                        Some(&error),
                    );
                }
            }
            Err(CheckEnd::Shutdown) => {}
            Err(CheckEnd::Step { step, error }) => {
                self.record_drive_failure(&work, step, error).await;
            }
        }
        if let Err(error) = self
            .cleanup_harness_check_agent(&work.agent_id, &mut shutdown)
            .await
        {
            crate::logging::record(
                &self.secret_policy_snapshot(),
                "daemon.harness_check_cleanup_failed",
                &[("harness", work.harness.as_str())],
                Some(&error),
            );
        }
        match result {
            Ok(()) => {
                crate::logging::record(
                    &self.secret_policy_snapshot(),
                    "harness.check_passed",
                    &[("harness", work.harness.as_str())],
                    None,
                );
                Ok(())
            }
            Err(CheckEnd::Shutdown) => Ok(()),
            Err(CheckEnd::Step { step, error }) => Err(Error::new(
                error.code,
                format!(
                    "harness {} self-check failed at {step}: {}",
                    work.harness, error.message
                ),
            )),
        }
    }

    /// Record one driven failure: quota refusals and a freshly held provider
    /// release the run for after the hold, transient capacity releases the
    /// claim for the next tick, and anything else is the version's run.
    async fn record_drive_failure(&self, work: &HarnessCheckWork, step: &str, error: &Error) {
        let now = now_ms();
        let quota =
            eliot_core::is_quota_refusal(&error.code) || self.check_held_for_drive(work).await;
        let mut store = self.inner.store.lock().await;
        let record = if quota {
            store.record_harness_check_quota(&work.harness, &work.version, &work.profile, now)
        } else if matches!(
            error.code.as_str(),
            "busy" | "unavailable" | "capacity_unavailable"
        ) {
            store
                .release_harness_check_claim(&work.harness, &work.version, now)
                .map(|_| ())
                .map_err(|_| Error::new("internal", "harness check claim release failed"))
        } else {
            store.record_harness_check_failed(
                &work.harness,
                &work.version,
                &work.profile,
                step,
                &check_error_text(error),
                now,
            )
        };
        if let Err(record_error) = record {
            crate::logging::record(
                &self.secret_policy_snapshot(),
                "daemon.harness_check_failure_unrecorded",
                &[("harness", work.harness.as_str())],
                Some(&record_error),
            );
        }
    }

    /// True when the check profile's provider scope is currently held: a
    /// mid-run turn failure under a fresh hold is quota, not harness failure.
    /// The probe time stays empty so the read binds no SEND.
    async fn check_held_for_drive(&self, work: &HarnessCheckWork) -> bool {
        let snapshot = match self.resolve_profile_cached(&work.profile) {
            Ok(snapshot) => snapshot,
            Err(_) => return false,
        };
        self.inner
            .store
            .lock()
            .await
            .routing_refusal(&work.profile, &work.harness, &snapshot, None)
            .ok()
            .flatten()
            .is_some_and(|refusal| refusal.code == "provider_held")
    }

    /// The five scenario steps after launch: tool-call turn, steer, stop,
    /// resume. Each step waits inside the run's hard budget.
    async fn run_check_scenario(
        &self,
        work: &HarnessCheckWork,
        shutdown: &mut watch::Receiver<bool>,
    ) -> CheckResult<()> {
        self.wait_for_check_command(
            &work.launch_command_id,
            "launch",
            true,
            shutdown,
            work.deadline,
        )
        .await?
        .ok_or(CheckEnd::Shutdown)?;
        // Steer needs a live turn: submit the steer as soon as the scenario
        // turn starts, while the Agent is still busy or waiting.
        let steer_id = self
            .submit_check_steer(work, shutdown)
            .await?
            .ok_or(CheckEnd::Shutdown)?;
        // The scenario turn must succeed and actually use a tool.
        let (_, turn) = self
            .wait_for_check_command(
                &work.send_command_id,
                "turn_with_tool_call",
                false,
                shutdown,
                work.deadline,
            )
            .await?
            .ok_or(CheckEnd::Shutdown)?;
        let turn = turn.ok_or_else(|| {
            CheckEnd::fail(
                "turn_with_tool_call",
                "unknown",
                "harness check SEND left no provable turn outcome",
            )
        })?;
        check_turn_verdict(&turn, "turn_with_tool_call")?;
        let calls = {
            let store = self.inner.store.lock().await;
            store
                .turn_tool_calls(&work.agent_id, turn.generation, &turn.turn_id)
                .map_err(|error| {
                    CheckEnd::fail("turn_with_tool_call", error.code, *error.message)
                })?
        };
        if calls < 1 {
            return Err(CheckEnd::fail(
                "turn_with_tool_call",
                "harness_vendor",
                "the harness completed the scenario turn without a tool call",
            ));
        }
        // The steer must have applied to the live turn.
        self.wait_for_check_command(&steer_id, "steer", true, shutdown, work.deadline)
            .await?
            .ok_or(CheckEnd::Shutdown)?;
        // Stop the scenario Agent, then resume it for a final turn.
        let agent = self.check_agent(&work.agent_id).await?;
        let stop = self
            .submit_check_control(
                "stop",
                &work.agent_id,
                agent.generation,
                None,
                shutdown,
                work.deadline,
            )
            .await?
            .ok_or(CheckEnd::Shutdown)?;
        self.wait_for_check_command(&stop.command.id, "stop", true, shutdown, work.deadline)
            .await?
            .ok_or(CheckEnd::Shutdown)?;
        let resume = self
            .submit_check_control(
                "resume",
                &work.agent_id,
                agent.generation,
                Some(eliot_core::HARNESS_CHECK_RESUME_TEXT),
                shutdown,
                work.deadline,
            )
            .await?
            .ok_or(CheckEnd::Shutdown)?;
        self.wait_for_check_command(&resume.command.id, "resume", true, shutdown, work.deadline)
            .await?
            .ok_or(CheckEnd::Shutdown)?;
        let send = resume.linked_command.ok_or_else(|| {
            CheckEnd::fail(
                "resume",
                "source_integrity",
                "harness check resume did not admit its linked SEND",
            )
        })?;
        let (_, resumed) = self
            .wait_for_check_command(&send.id, "resume", false, shutdown, work.deadline)
            .await?
            .ok_or(CheckEnd::Shutdown)?;
        let resumed = resumed.ok_or_else(|| {
            CheckEnd::fail(
                "resume",
                "unknown",
                "harness check resume left no provable turn outcome",
            )
        })?;
        check_turn_verdict(&resumed, "resume")?;
        Ok(())
    }

    /// The live check Agent, or a step failure when it disappeared.
    async fn check_agent(&self, agent_id: &str) -> CheckResult<Agent> {
        let store = self.inner.store.lock().await;
        store
            .get_agent(agent_id)
            .map_err(|error| CheckEnd::fail("launch", error.code, *error.message))
            .and_then(|agent| {
                agent.ok_or_else(|| {
                    CheckEnd::fail("launch", "unknown", "harness check Agent disappeared")
                })
            })
    }

    /// Submit one targeted control (steer, stop, resume) for the check Agent.
    /// Returns `None` on daemon shutdown before admission.
    async fn submit_check_control(
        &self,
        verb: &str,
        agent_id: &str,
        generation: i64,
        text: Option<&str>,
        shutdown: &mut watch::Receiver<bool>,
        deadline: Instant,
    ) -> CheckResult<Option<Admission>> {
        let step = verb_step(verb);
        if *shutdown.borrow() {
            return Ok(None);
        }
        if Instant::now() >= deadline {
            return Err(CheckEnd::fail(
                step,
                "timeout",
                "harness check exceeded its time budget",
            ));
        }
        let mut command = SubmitCommand::new(ulid::Ulid::new().to_string(), verb);
        command.target = Some(agent_id.to_owned());
        command.expected_generation = Some(generation);
        if let Some(text) = text {
            command.args = json!({"text": text});
        }
        let prepared = self
            .prepare_submission(command, &Principal::Owner)
            .await
            .map_err(|error| CheckEnd::fail(step, error.code, *error.message))?;
        if *shutdown.borrow() {
            return Ok(None);
        }
        let admission = self
            .admit_check_command(prepared)
            .await
            .map_err(|error| CheckEnd::fail(step, error.code, *error.message))?;
        Ok(Some(admission))
    }

    /// Submit the scenario steer as soon as the SEND binds a live turn.
    /// Native turn events do not always wake the daemon promptly, so the arm
    /// polls on a short interval instead of waiting for progress. A `bad_state`
    /// rejection re-reads the Agent rather than matching message text: a turn
    /// that ended in the gap reads as terminal SEND and fails the step, while
    /// a still-live turn retries inside the arm window. Returns `None` on
    /// shutdown.
    async fn submit_check_steer(
        &self,
        work: &HarnessCheckWork,
        shutdown: &mut watch::Receiver<bool>,
    ) -> CheckResult<Option<String>> {
        let arm_by = Instant::now() + CHECK_STEER_ARM_WAIT;
        loop {
            if *shutdown.borrow() {
                return Ok(None);
            }
            if Instant::now() >= arm_by.min(work.deadline) {
                return Err(CheckEnd::fail(
                    "steer",
                    "harness_vendor",
                    "the harness started no live turn to steer",
                ));
            }
            let live = {
                let store = self.inner.store.lock().await;
                let agent = store
                    .get_agent(&work.agent_id)
                    .map_err(|error| {
                        CheckEnd::fail("turn_with_tool_call", error.code, *error.message)
                    })?
                    .ok_or_else(|| {
                        CheckEnd::fail(
                            "turn_with_tool_call",
                            "unknown",
                            "harness check Agent disappeared",
                        )
                    })?;
                let send = store
                    .get_command(&work.send_command_id)
                    .map_err(|error| {
                        CheckEnd::fail("turn_with_tool_call", error.code, *error.message)
                    })?
                    .ok_or_else(|| {
                        CheckEnd::fail(
                            "turn_with_tool_call",
                            "unknown",
                            "harness check SEND disappeared",
                        )
                    })?;
                if agent.active_turn.is_some() {
                    true
                } else if send.state.is_terminal() {
                    if send.state != CommandState::Applied {
                        return Err(check_command_failure(&send, "turn_with_tool_call"));
                    }
                    if let (Some(generation), Some(turn_id)) =
                        (send.generation, send.turn_id.as_deref())
                        && let Some(turn) = store
                            .get_turn(&work.agent_id, generation, turn_id)
                            .map_err(|error| {
                                CheckEnd::fail("turn_with_tool_call", error.code, *error.message)
                            })?
                    {
                        check_turn_verdict(&turn, "turn_with_tool_call")?;
                    }
                    return Err(CheckEnd::fail(
                        "steer",
                        "harness_vendor",
                        "the scenario turn ended before the steer was accepted",
                    ));
                } else {
                    false
                }
            };
            if live {
                match self
                    .submit_check_control(
                        "steer",
                        &work.agent_id,
                        self.check_agent(&work.agent_id).await?.generation,
                        Some(eliot_core::HARNESS_CHECK_STEER_TEXT),
                        shutdown,
                        work.deadline,
                    )
                    .await
                {
                    Ok(Some(admission)) => return Ok(Some(admission.command.id)),
                    // Shutdown, or the turn ended between the sighting and
                    // admission: re-read on the next pass instead of matching
                    // message text.
                    Ok(None) => continue,
                    Err(CheckEnd::Step { error, .. }) if error.code == "bad_state" => continue,
                    Err(other) => return Err(other),
                }
            }
            #[expect(
                clippy::disallowed_methods,
                reason = "This bounded arm poll is owned by the surrounding check run."
            )]
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    /// Wait for one check command inside the run budget. With
    /// `command_settles`, a terminal command ends the wait even while its
    /// turn still runs (steer, stop, resume); otherwise the bound turn's
    /// outcome rules (SEND). Failures carry `step`. Returns `None` on
    /// shutdown.
    async fn wait_for_check_command(
        &self,
        command_id: &str,
        step: &'static str,
        command_settles: bool,
        shutdown: &mut watch::Receiver<bool>,
        deadline: Instant,
    ) -> CheckResult<Option<(Command, Option<Turn>)>> {
        let mut progress = self.inner.progress.subscribe();
        loop {
            if *shutdown.borrow() {
                return Ok(None);
            }
            let _ = *progress.borrow_and_update();
            let marks = self.wait_unknown_marks().await;
            let readback = {
                let store = self.inner.store.lock().await;
                let Some(command) = store
                    .get_command(command_id)
                    .map_err(|error| CheckEnd::fail(step, error.code, *error.message))?
                else {
                    return Err(CheckEnd::fail(
                        step,
                        "unknown",
                        "harness check command disappeared",
                    ));
                };
                if command.state == CommandState::Unknown || marks.causes.contains_key(&command.id)
                {
                    return Err(check_command_failure(&command, step));
                }
                if command.state.is_terminal() && command.state != CommandState::Applied {
                    return Err(check_command_failure(&command, step));
                }
                if command_settles && command.state.is_terminal() {
                    (command, None)
                } else {
                    let turn = if let (Some(agent), Some(generation), Some(turn_id)) = (
                        command.target.as_deref(),
                        command.generation,
                        command.turn_id.as_deref(),
                    ) {
                        let fetched = store
                            .get_turn(agent, generation, turn_id)
                            .map_err(|error| CheckEnd::fail(step, error.code, *error.message))?;
                        // A turn the SEND completed with a definite outcome is
                        // provable even when a later foreign command (like the
                        // scenario steer) sits unsettled on the same turn: the
                        // foreign command cannot un-complete it. Anything less
                        // than Applied-plus-definite keeps the unknown mask.
                        if !command_settles
                            && command.state == CommandState::Applied
                            && fetched.as_ref().is_some_and(|turn| {
                                !matches!(
                                    Self::turn_entry_status(turn),
                                    "still_running" | "unknown"
                                )
                            })
                        {
                            fetched
                        } else {
                            let turn_ids = [turn_id.to_owned()];
                            if store
                                .turns_with_unknown_commands(
                                    agent,
                                    generation,
                                    &turn_ids,
                                    &marks.memory,
                                )
                                .map_err(|error| CheckEnd::fail(step, error.code, *error.message))?
                                .contains(turn_id)
                            {
                                return Err(CheckEnd::fail(
                                    step,
                                    "unknown",
                                    "harness check turn outcome is unknown",
                                ));
                            }
                            fetched
                        }
                    } else {
                        None
                    };
                    (command, turn)
                }
            };
            let (command, turn) = readback;
            if let Some(turn) = turn.as_ref() {
                match Self::turn_entry_status(turn) {
                    "still_running" => {}
                    _ => return Ok(Some((command, Some(turn.clone())))),
                }
            } else if command.state.is_terminal() {
                return Ok(Some((command, None)));
            }
            if Instant::now() >= deadline {
                return Err(CheckEnd::fail(
                    step,
                    "timeout",
                    "harness check exceeded its time budget",
                ));
            }
            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => {}
                changed = progress.changed() => {
                    if changed.is_err() {
                        return Err(CheckEnd::fail(step, "unavailable", "harness check progress closed"));
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(None);
                    }
                }
            }
        }
    }

    /// Always attempt cleanup after a driven check: stop a live Agent, then
    /// delete it through the ordinary Owner path.
    pub(crate) async fn cleanup_harness_check_agent(
        &self,
        agent_id: &str,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<()> {
        let current = {
            let store = self.inner.store.lock().await;
            store.get_agent(agent_id)?
        };
        let Some(agent) = current else {
            return Ok(());
        };
        if !agent.state.is_terminal() {
            let mut stop = SubmitCommand::new(ulid::Ulid::new().to_string(), "stop");
            stop.target = Some(agent.id.clone());
            stop.expected_generation = Some(agent.generation);
            if let Ok(prepared) = self.prepare_submission(stop, &Principal::Owner).await
                && let Ok(admission) = self.admit_check_command(prepared).await
            {
                let stop_by = Instant::now() + CHECK_CLEANUP_WAIT;
                let _ = tokio::time::timeout_at(
                    stop_by,
                    self.wait_for_check_command(
                        &admission.command.id,
                        "stop",
                        true,
                        shutdown,
                        stop_by,
                    ),
                )
                .await;
            }
        }
        let request = eliot_core::api::AgentDeleteRequest {
            id: ulid::Ulid::new().to_string(),
            agent: agent_id.to_owned(),
        };
        let params = serde_json::to_value(request)
            .map_err(|_| Error::new("internal", "harness check cleanup request is invalid"))?;
        let id = required_text(&params, "id")?;
        let agent = required_text(&params, "agent")?;
        let deadline = tokio::time::Instant::now() + CHECK_CLEANUP_WAIT;
        loop {
            let inspected = {
                let mut store = self.inner.store.lock().await;
                store.inspect_agent_delete(id, agent, now_ms())
            };
            match inspected {
                Ok(_) => break,
                Err(error) if error.code == "busy" && tokio::time::Instant::now() < deadline => {
                    #[expect(
                        clippy::disallowed_methods,
                        reason = "This bounded wait is owned by the surrounding operation."
                    )]
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(error) => return Err(error),
            }
        }
        self.agent_delete(&params, &Principal::Owner).await?;
        Ok(())
    }

    async fn admit_check_command(&self, prepared: PreparedSubmission) -> Result<Admission> {
        let admission = {
            let mut store = self.inner.store.lock().await;
            let admission = self.admit_prepared_submission(
                &mut store,
                "owner",
                prepared,
                SubmissionContext::default(),
            )?;
            self.publish_locked(&admission.events);
            admission
        };
        if admission.command.state == CommandState::Unknown {
            self.inner
                .unknown_commands
                .lock_recover()
                .insert(admission.command.id.clone());
        }
        if admission.is_new && command_needs_worker(&admission.command) {
            self.schedule_admission(&admission);
        }
        self.signal_progress();
        Ok(admission)
    }
}

pub(super) fn configure_check_snapshot(snapshot: &mut Value, harness: &str, version: &str) {
    if let Some(fields) = snapshot.as_object_mut() {
        fields.remove("collaboration_mode");
        fields.remove("setup");
    }
    snapshot[eliot_core::HARNESS_CHECK_LAUNCH_ARG] =
        json!({"harness": harness, "version": version});
    snapshot["permissions"] = json!("read-only");
    snapshot["approval_policy"] = json!("never");
    snapshot["native_subagents"] = json!("deny");
    snapshot["mcp"] = json!("minimal");
    snapshot["mcp_allow"] = json!([]);
}

fn check_command_failure(command: &Command, step: &'static str) -> CheckEnd {
    let outcome = command.outcome.as_ref();
    let text = |field| {
        outcome
            .and_then(|outcome| outcome.get(field))
            .and_then(Value::as_str)
    };
    CheckEnd::fail(
        step,
        text("code").or_else(|| text("reason")).unwrap_or("unknown"),
        text("message").unwrap_or("harness check command ended without a proven result"),
    )
}

/// How a driven scenario ends: clean shutdown (record nothing), or a step
/// failure carrying the step name and the boxed error to record. The error
/// rides boxed so driven-step waits stay under the large-`Err` lint.
enum CheckEnd {
    Shutdown,
    Step {
        step: &'static str,
        error: Box<Error>,
    },
}

impl CheckEnd {
    fn fail(step: &'static str, code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Step {
            step,
            error: Box::new(Error::new(code, message)),
        }
    }
}

/// The scenario step a targeted control verb exercises.
fn verb_step(verb: &str) -> &'static str {
    match verb {
        "steer" => "steer",
        "stop" => "stop",
        "resume" => "resume",
        _ => "launch",
    }
}

/// Accept only a successful turn. Anything else is the step's failure with
/// the vendor's terminal message when the turn kept one.
fn check_turn_verdict(turn: &Turn, step: &'static str) -> CheckResult<()> {
    match Router::turn_entry_status(turn) {
        "succeeded" => Ok(()),
        "still_running" => Err(CheckEnd::fail(
            step,
            "internal",
            "harness check read a running turn as settled",
        )),
        _ => Err(CheckEnd::fail(
            step,
            "harness_vendor",
            turn.native_error_message
                .as_deref()
                .filter(|message| !message.trim().is_empty())
                .map_or_else(
                    || "the harness ended the scenario turn without success".to_owned(),
                    str::to_owned,
                ),
        )),
    }
}

/// Bounded failure text for the stored check record: the error message cut
/// to the stored limit on a character boundary. Only Swarm-authored messages
/// and already-persisted native terminal text reach this path.
fn check_error_text(error: &Error) -> String {
    let mut end = error
        .message
        .len()
        .min(eliot_core::MAX_HARNESS_CHECK_ERROR_BYTES);
    while !error.message.is_char_boundary(end) {
        end -= 1;
    }
    error.message[..end].to_owned()
}
