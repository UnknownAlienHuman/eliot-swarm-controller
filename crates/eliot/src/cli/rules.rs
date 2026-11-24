//! Owner-state commands: provider, project, rule, timer, goal.
use super::args::{
    GoalArgs, GoalClearArgs, GoalCommand, GoalPauseArgs, GoalResumeArgs, GoalRoundArgs,
    GoalSetArgs, GoalShowArgs, ProjectArgs, ProjectCommand, ProjectShowArgs, ProviderArgs,
    ProviderClearHoldArgs, ProviderCommand, ProviderHoldsArgs, RuleArgs, RuleCatchUpArgs,
    RuleCommand, RuleFileArgs, RuleShowArgs, TimerArgs, TimerCommand, TimerListArgs, TimerRunArgs,
    TimerShowArgs,
};
use super::support::read_and_print;
use super::support::{
    Failure, command_value, mutation_id, print_value, print_value_or_human, response_exit_code,
};
use eliot_core::Command as CoreCommand;
use eliot_core::api::{
    GoalClearRequest, GoalCostLimit, GoalDefinition, GoalEscalation, GoalGetRequest, GoalMode,
    GoalPauseRequest, GoalResumeRequest, GoalRoundRequest, GoalRoundResponse, GoalSetRequest,
    GoalView, MAX_RULE_FILE_BYTES, ProjectOverviewRequest, ProjectOverviewResponse,
    ProjectRegistrationView, ProviderClearHoldRequest, ProviderClearHoldResponse,
    ProviderCorrectiveEvidence, ProviderHoldsRequest, ProviderHoldsResponse, RuleCatchUpRequest,
    RuleDeleteRequest, RuleDeleteResponse, RuleGetRequest, RuleListEntry, RuleListRequest,
    RuleListResponse, RuleReadRequest, RuleReadResponse, RuleSaveRequest, RuleSaveResponse,
    RuleShowView, TimerGetRequest, TimerRunRequest, TimerSchedule, TimerStateView,
};
use serde_json::Value;
use serde_json::json;
use std::{fs::File, io::Read, path::PathBuf};

fn print_rule_list(response: &RuleListResponse) {
    if response.rules.is_empty() {
        println!("No rules in this page.");
    } else {
        println!("Rules ({}):", response.rules.len());
        for entry in &response.rules {
            let state = &entry.state;
            let status = if !state.enabled {
                "disabled"
            } else if state.paused_reason.is_some() {
                "paused"
            } else {
                "enabled"
            };
            let project = state.project.as_deref().unwrap_or("global");
            println!(
                "  {} · {} · project {} · revision {} · cursor {}",
                state.id, status, project, state.revision, state.current_cursor
            );
            if let Some(reason) = state.paused_reason.as_deref() {
                println!("    paused: {reason}");
            }
            if !entry.source_present {
                println!("    source: missing from the current configuration");
            } else if let Some(path) = entry.source_file.as_deref() {
                println!("    source: {path}");
            }
        }
    }
    if let Some(next) = response.next.as_deref() {
        println!("Continue with --after {next}");
    }
}

fn print_timer_list(entries: &[RuleListEntry]) {
    if entries.is_empty() {
        println!("No timers in this page.");
        return;
    }
    println!("Timers ({}):", entries.len());
    for entry in entries {
        if let Some(timer) = entry.timer.as_ref() {
            print_timer_summary(timer);
        }
    }
    if let Some(next) = entries.last().map(|entry| entry.state.id.as_str()) {
        println!("Page cursor (use with --after): {next}");
    }
}

fn print_timer_summary(timer: &TimerStateView) {
    let status = if timer.enabled { "enabled" } else { "disabled" };
    let scope = timer.project.as_deref().unwrap_or("global");
    let due = timer
        .next_due_at
        .map(|at| format!("{at} ms since Unix epoch"))
        .unwrap_or_else(|| "not scheduled".to_owned());
    println!(
        "  {} · {} · scope {} · {} · next due {}",
        timer.id,
        status,
        scope,
        timer_schedule_text(&timer.schedule),
        due
    );
    if let Some(outcome) = timer.last_outcome.as_deref() {
        let description = match outcome {
            "brief_settling" => "Briefs are still settling",
            "brief_requires_include" => "Older briefs require explicit Owner inclusion",
            "not_found" => "No new brief",
            "capacity_unavailable" => "Waiting for capacity",
            other => other,
        };
        println!("    Last scheduled outcome: {description}");
    }
}

fn timer_schedule_text(schedule: &TimerSchedule) -> String {
    match schedule {
        TimerSchedule::Interval { period_ms, .. } => format!("every {period_ms} ms"),
        TimerSchedule::Cron { expression, tz } => format!("cron {expression} ({tz})"),
        TimerSchedule::Once { at } => format!("once at {at} ms UTC"),
    }
}

fn print_goal_summary(goal: &GoalView) {
    println!(
        "Goal for {} · {} · generation {} · {}",
        goal.agent,
        goal.mode.as_str(),
        goal.generation,
        goal.status.as_str()
    );
    println!("{}", goal.definition.text);
    println!("Check: {}", goal.definition.check.join(" "));
    let turns = goal
        .definition
        .max_turns
        .map(|max| format!("{}/{}", goal.counters.turns_used, max))
        .unwrap_or_else(|| goal.counters.turns_used.to_string());
    let rounds = goal
        .definition
        .max_rounds
        .map(|max| format!("{}/{}", goal.counters.rounds, max))
        .unwrap_or_else(|| goal.counters.rounds.to_string());
    println!("Progress: {turns} turns · {rounds} rounds");
    if let Some(reason) = goal.blocked_reason.as_deref() {
        println!("Blocked: {reason}");
    }
    if let Some(reason) = goal.paused_reason.as_deref() {
        println!("Paused: {reason}");
    }
    println!("Cost: {}", goal.cost_status);
    if let Some(at) = goal.cost_baseline_observed_at {
        println!("Cost baseline snapshot observed at {at} ms since Unix epoch");
    }
}

pub(crate) async fn run_provider(
    ProviderArgs { command }: ProviderArgs,
    data_dir: PathBuf,
    json_output: bool,
    id: Option<String>,
) -> Result<u8, Failure> {
    match command {
        ProviderCommand::Holds(ProviderHoldsArgs { after, limit }) => {
            let request = ProviderHoldsRequest { after, limit };
            request.validate().map_err(Failure::for_read)?;
            let response: ProviderHoldsResponse =
                eliot_daemon::client_call_typed(&data_dir, "provider.holds", &request)
                    .await
                    .map_err(Failure::for_read)?;
            request
                .validate_response(&response)
                .map_err(Failure::invalid_response)?;
            let value = serde_json::to_value(response)
                .map_err(|_| Failure::new(4, "provider.holds response is invalid"))?;
            print_value(&value, json_output);
            Ok(0)
        }
        ProviderCommand::ClearHold(ProviderClearHoldArgs {
            scope,
            corrective_evidence,
            expected_probe_command,
        }) => {
            let corrective_evidence: ProviderCorrectiveEvidence =
                serde_json::from_value(Value::String(corrective_evidence))
                    .map_err(|_| Failure::new(2, "invalid provider corrective evidence"))?;
            let request = ProviderClearHoldRequest {
                id: mutation_id(id)?,
                scope,
                corrective_evidence,
                expected_probe_command,
            };
            request.validate().map_err(Failure::for_mutation)?;
            let response: ProviderClearHoldResponse =
                eliot_daemon::client_call_typed(&data_dir, "provider.clear_hold", &request)
                    .await
                    .map_err(Failure::for_mutation)?;
            request
                .validate_response(&response)
                .map_err(Failure::invalid_response)?;
            let value = serde_json::to_value(response)
                .map_err(|_| Failure::new(4, "provider.clear_hold response is invalid"))?;
            print_value(&value, json_output);
            Ok(0)
        }
    }
}

pub(crate) async fn run_project(
    ProjectArgs { command }: ProjectArgs,
    data_dir: PathBuf,
    json_output: bool,
    id: Option<String>,
) -> Result<u8, Failure> {
    match command {
        ProjectCommand::Transfer(args) => {
            super::project_assignment::transfer(args, &data_dir, json_output, id).await
        }
        ProjectCommand::Assignment(args) => {
            super::project_assignment::run(args.command, &data_dir, json_output, id).await
        }
        ProjectCommand::Broadcast { project, text } => {
            super::project_broadcast::run(project, text, &data_dir, json_output, id).await
        }
        ProjectCommand::Budget {
            project,
            revision,
            max_live,
            priority,
            pause,
            resume,
        } => {
            let request = eliot_core::api::ManagerBudgetRequest {
                id: mutation_id(id)?,
                project,
                expected_revision: revision,
                max_live,
                priority,
                paused: (pause || resume).then_some(pause),
            };
            request.validate().map_err(Failure::for_mutation)?;
            let reply: eliot_core::api::ManagerBudgetView =
                eliot_daemon::client_call_typed(&data_dir, "project.budget.set", &request)
                    .await
                    .map_err(Failure::for_mutation)?;
            if reply.project != request.project {
                return Err(Failure::new(
                    4,
                    "project.budget.set response project differs",
                ));
            }
            print_value(&json!(reply), json_output);
            Ok(0)
        }
        ProjectCommand::List => {
            let response = eliot_daemon::client_call(&data_dir, "project.list", json!({}))
                .await
                .map_err(Failure::for_read)?;
            let Some(projects) = response.as_array() else {
                return Err(Failure::new(4, "project.list response is invalid"));
            };
            let projects: Vec<ProjectRegistrationView> =
                serde_json::from_value(Value::Array(projects.clone()))
                    .map_err(|_| Failure::new(4, "project.list response is invalid"))?;
            for project in &projects {
                project
                    .validate()
                    .map_err(|_| Failure::new(4, "project.list response is invalid"))?;
            }
            print_value(&response, json_output);
            Ok(0)
        }
        ProjectCommand::Overview { project, after } => {
            let request = ProjectOverviewRequest { project, after };
            request
                .validate()
                .map_err(|error| Failure::from_core(error, true))?;
            let response: ProjectOverviewResponse =
                eliot_daemon::client_call_typed(&data_dir, "project.overview", &request)
                    .await
                    .map_err(|error| Failure::from_core(error, false))?;
            request
                .validate_response(&response)
                .map_err(|_| Failure::new(4, "project.overview response is invalid"))?;
            let value = serde_json::to_value(response)
                .map_err(|_| Failure::new(4, "project.overview response is invalid"))?;
            print_value(&value, json_output);
            Ok(0)
        }
        ProjectCommand::Show(ProjectShowArgs { id }) => {
            let response: ProjectRegistrationView =
                eliot_daemon::client_call_typed(&data_dir, "project.get", &json!({"id":id}))
                    .await
                    .map_err(Failure::for_read)?;
            if response.id != id || response.validate().is_err() {
                return Err(Failure::new(4, "project.get response is invalid"));
            }
            let value = serde_json::to_value(&response)
                .map_err(|_| Failure::new(4, "project.get response is invalid"))?;
            print_value_or_human(&value, json_output, || {
                let repository = response.origin.as_deref().unwrap_or("unknown repository");
                println!("Project {} ({repository})", response.id);
                println!("Checkout: {}", response.checkout);
                println!(
                    "Profiles: {}",
                    if response.profiles.is_empty() {
                        "none".to_owned()
                    } else {
                        response.profiles.join(", ")
                    }
                );
                println!(
                    "Actions: {}",
                    if response.actions.is_empty() {
                        "none".to_owned()
                    } else {
                        response.actions.join(", ")
                    }
                );
                println!("Revision: {}", response.revision);
                println!(
                    "policy applied to {} of {} Agents in the last 24 h",
                    response.policy_usage.applied, response.policy_usage.total
                );
                if !response.policy_usage.opt_outs.is_empty() {
                    println!(
                        "Policy opt-outs (showing {} of {})",
                        response.policy_usage.opt_outs.len(),
                        response.policy_usage.opted_out
                    );
                    for opt_out in &response.policy_usage.opt_outs {
                        println!("  {}: {}", opt_out.agent, opt_out.reason);
                    }
                }
                if !response.checkouts.is_empty() {
                    println!("Registered checkouts: {}", response.checkouts.len());
                    for checkout in &response.checkouts {
                        let branch = checkout.branch.as_deref().unwrap_or("detached");
                        println!(
                            "  {}{} ({branch})",
                            checkout.path,
                            if checkout.main { " (main)" } else { "" }
                        );
                    }
                }
            });
            Ok(0)
        }
    }
}

pub(crate) async fn run_rule(
    RuleArgs { command }: RuleArgs,
    data_dir: PathBuf,
    json_output: bool,
    id: Option<String>,
) -> Result<u8, Failure> {
    match command {
        RuleCommand::List {
            project,
            after,
            limit,
        } => {
            let request = RuleListRequest {
                project,
                after,
                limit,
            };
            request.validate().map_err(Failure::for_read)?;
            let response: RuleListResponse =
                eliot_daemon::client_call_typed(&data_dir, "rule.list", &request)
                    .await
                    .map_err(Failure::for_read)?;
            request
                .validate_response(&response)
                .map_err(Failure::invalid_response)?;
            let value = serde_json::to_value(&response)
                .map_err(|_| Failure::new(4, "rule.list response is invalid"))?;
            print_value_or_human(&value, json_output, || print_rule_list(&response));
            Ok(0)
        }
        RuleCommand::Show(RuleShowArgs { id, project }) => {
            let request = match project {
                Some(project) => RuleGetRequest::new(id).in_project(project),
                None => RuleGetRequest::new(id),
            };
            request.validate().map_err(Failure::for_read)?;
            let response: RuleShowView =
                eliot_daemon::client_call_typed(&data_dir, "rule.get", &request)
                    .await
                    .map_err(Failure::for_read)?;
            request
                .validate_show_response(&response)
                .map_err(Failure::invalid_response)?;
            let value = serde_json::to_value(response)
                .map_err(|_| Failure::new(4, "rule.get response is invalid"))?;
            print_value(&value, json_output);
            Ok(0)
        }
        RuleCommand::Read(RuleFileArgs { project, path }) => {
            let request = RuleReadRequest { project, path };
            read_and_print!(
                &data_dir,
                json_output,
                "rule.read",
                request,
                RuleReadResponse
            )
        }
        RuleCommand::Save {
            entry: RuleFileArgs { project, path },
            file,
            expected_revision,
        } => {
            let mut bytes = Vec::new();
            File::open(file)
                .and_then(|file| {
                    file.take(MAX_RULE_FILE_BYTES as u64 + 1)
                        .read_to_end(&mut bytes)
                })
                .map_err(|error| {
                    Failure::new(2, format!("cannot read rule input file ({})", error.kind()))
                })?;
            if bytes.len() > MAX_RULE_FILE_BYTES {
                return Err(Failure::new(
                    2,
                    "rule input file exceeds the rule file limit",
                ));
            }
            let content = String::from_utf8(bytes)
                .map_err(|_| Failure::new(2, "rule input file must be UTF-8"))?;
            let request = RuleSaveRequest {
                project,
                path,
                expected_revision,
                content,
            };
            request.validate().map_err(Failure::for_mutation)?;
            let response: RuleSaveResponse =
                eliot_daemon::client_call_typed(&data_dir, "rule.save", &request)
                    .await
                    .map_err(Failure::for_mutation)?;
            request
                .validate_response(&response)
                .map_err(Failure::invalid_response)?;
            print_value(&json!(response), json_output);
            Ok(0)
        }
        RuleCommand::Delete {
            entry: RuleFileArgs { project, path },
            expected_revision,
        } => {
            let request = RuleDeleteRequest {
                project,
                path,
                expected_revision,
            };
            request.validate().map_err(Failure::for_mutation)?;
            let response: RuleDeleteResponse =
                eliot_daemon::client_call_typed(&data_dir, "rule.delete", &request)
                    .await
                    .map_err(Failure::for_mutation)?;
            request
                .validate_response(&response)
                .map_err(Failure::invalid_response)?;
            print_value(&json!(response), json_output);
            Ok(0)
        }
        RuleCommand::CatchUp(RuleCatchUpArgs {
            rule,
            after,
            through,
            project,
        }) => {
            let request = RuleCatchUpRequest::new(mutation_id(id)?, rule, after, through);
            let request = match project {
                Some(project) => request.in_project(project),
                None => request,
            };
            request.validate().map_err(Failure::for_mutation)?;
            let response: CoreCommand =
                eliot_daemon::client_call_typed(&data_dir, "rule.catch_up", &request)
                    .await
                    .map_err(Failure::for_mutation)?;
            request
                .validate_response(&response)
                .map_err(Failure::for_mutation)?;
            let exit_code = response_exit_code(response.state);
            let value = command_value(response)?;
            print_value(&value, json_output);
            Ok(exit_code)
        }
    }
}

pub(crate) async fn run_timer(
    TimerArgs { command }: TimerArgs,
    data_dir: PathBuf,
    json_output: bool,
    id: Option<String>,
) -> Result<u8, Failure> {
    match command {
        TimerCommand::List(TimerListArgs {
            project,
            after,
            limit,
        }) => {
            let mut request = RuleListRequest {
                project,
                after,
                limit,
            };
            request.validate().map_err(Failure::for_read)?;
            // The RPC pages all rules. Fill a page of timer rows even if
            // another rule sorts before the first timer.
            request.limit = limit.max(RuleListRequest::default().limit);
            let mut timers = Vec::new();
            while timers.len() < limit {
                let response: RuleListResponse =
                    eliot_daemon::client_call_typed(&data_dir, "rule.list", &request)
                        .await
                        .map_err(Failure::for_read)?;
                request
                    .validate_response(&response)
                    .map_err(Failure::invalid_response)?;
                timers.extend(
                    response
                        .rules
                        .into_iter()
                        .filter(|entry| entry.timer.is_some())
                        .take(limit - timers.len()),
                );
                let Some(next) = response.next else { break };
                request.after = Some(next);
            }
            let next = timers.last().map(|entry| &entry.state.id);
            let value = json!({"timers": &timers, "next": next});
            print_value_or_human(&value, json_output, || print_timer_list(&timers));
            Ok(0)
        }
        TimerCommand::Show(TimerShowArgs { id, project }) => {
            let request = match project {
                Some(project) => TimerGetRequest::new(id).in_project(project),
                None => TimerGetRequest::new(id),
            };
            request.validate().map_err(Failure::for_read)?;
            let timer: TimerStateView =
                eliot_daemon::client_call_typed(&data_dir, "timer.get", &request)
                    .await
                    .map_err(Failure::for_read)?;
            request
                .validate_response(&timer)
                .map_err(Failure::invalid_response)?;
            let value = serde_json::to_value(&timer)
                .map_err(|_| Failure::new(4, "timer.get response is invalid"))?;
            print_value_or_human(&value, json_output, || print_timer_summary(&timer));
            Ok(0)
        }
        TimerCommand::Run(TimerRunArgs {
            rule,
            project,
            include,
        }) => {
            let mut request = TimerRunRequest::new(mutation_id(id)?, rule);
            request.include = include;
            let request = match project {
                Some(project) => request.in_project(project),
                None => request,
            };
            request.validate().map_err(Failure::for_mutation)?;
            let response: CoreCommand =
                eliot_daemon::client_call_typed(&data_dir, "timer.run", &request)
                    .await
                    .map_err(Failure::for_mutation)?;
            request
                .validate_response(&response)
                .map_err(Failure::for_mutation)?;
            let exit_code = response_exit_code(response.state);
            let value = command_value(response)?;
            print_value(&value, json_output);
            Ok(exit_code)
        }
    }
}

/// A decimal amount such as `0.25` in millionths, without float rounding.
fn cost_amount_micros(text: &str) -> Result<i64, eliot_core::Error> {
    let invalid = || {
        eliot_core::Error::new(
            "invalid",
            "--max-cost takes a decimal amount with at most 6 fractional digits",
        )
    };
    let (whole, fraction) = text.split_once('.').unwrap_or((text, ""));
    if whole.is_empty() && fraction.is_empty()
        || fraction.len() > 6
        || !whole
            .chars()
            .chain(fraction.chars())
            .all(|c| c.is_ascii_digit())
    {
        return Err(invalid());
    }
    let whole: i64 = if whole.is_empty() {
        0
    } else {
        whole.parse().map_err(|_| invalid())?
    };
    let fraction: i64 = format!("{fraction:0<6}").parse().map_err(|_| invalid())?;
    whole
        .checked_mul(1_000_000)
        .and_then(|micros| micros.checked_add(fraction))
        .ok_or_else(invalid)
}

pub(crate) async fn run_goal(
    GoalArgs { command }: GoalArgs,
    data_dir: PathBuf,
    json_output: bool,
    id: Option<String>,
) -> Result<u8, Failure> {
    match command {
        GoalCommand::Set(args) => {
            let GoalSetArgs {
                agent,
                mode,
                text,
                check,
                check_cwd,
                check_timeout_ms,
                check_timeout_retry_safe,
                max_turns,
                max_rounds,
                max_without_progress,
                cadence_ms,
                scope,
                check_inputs,
                max_cost,
                cost_unit,
                escalate_model,
                escalate_effort,
                escalate_reason,
            } = *args;
            let escalation = escalate_reason.map(|reason| GoalEscalation {
                model: escalate_model,
                effort: escalate_effort,
                reason,
            });
            let max_cost = match (max_cost, cost_unit) {
                (Some(amount), Some(unit)) => Some(GoalCostLimit {
                    unit,
                    amount_micros: cost_amount_micros(&amount).map_err(Failure::for_mutation)?,
                }),
                _ => None,
            };
            let request = GoalSetRequest {
                id: mutation_id(id)?,
                agent,
                mode: goal_mode(&mode)?,
                definition: GoalDefinition {
                    text,
                    check,
                    check_cwd,
                    check_timeout_ms,
                    check_timeout_retry_safe,
                    max_turns,
                    max_rounds,
                    max_without_progress,
                    cadence_ms,
                    scope,
                    policy_scope: Vec::new(),
                    check_inputs,
                    max_cost,
                    escalation,
                },
            };
            request.validate().map_err(Failure::for_mutation)?;
            let response: GoalView =
                eliot_daemon::client_call_typed(&data_dir, "goal.set", &request)
                    .await
                    .map_err(Failure::for_mutation)?;
            request
                .validate_response(&response)
                .map_err(Failure::for_mutation)?;
            let value = serde_json::to_value(response)
                .map_err(|_| Failure::new(4, "goal.set response is invalid"))?;
            print_value(&value, json_output);
            Ok(0)
        }
        GoalCommand::Show(GoalShowArgs { agent, mode }) => {
            let request = GoalGetRequest {
                agent,
                mode: goal_mode(&mode)?,
            };
            request.validate().map_err(Failure::for_read)?;
            let response: GoalView =
                eliot_daemon::client_call_typed(&data_dir, "goal.get", &request)
                    .await
                    .map_err(Failure::for_read)?;
            request
                .validate_response(&response)
                .map_err(Failure::invalid_response)?;
            let value = serde_json::to_value(&response)
                .map_err(|_| Failure::new(4, "goal.get response is invalid"))?;
            print_value_or_human(&value, json_output, || print_goal_summary(&response));
            Ok(0)
        }
        GoalCommand::Pause(GoalPauseArgs {
            agent,
            mode,
            reason,
        }) => {
            let request = GoalPauseRequest {
                id: mutation_id(id)?,
                agent,
                mode: goal_mode(&mode)?,
                reason,
            };
            request.validate().map_err(Failure::for_mutation)?;
            let response: GoalView =
                eliot_daemon::client_call_typed(&data_dir, "goal.pause", &request)
                    .await
                    .map_err(Failure::for_mutation)?;
            request
                .validate_response(&response)
                .map_err(Failure::for_mutation)?;
            let value = serde_json::to_value(response)
                .map_err(|_| Failure::new(4, "goal.pause response is invalid"))?;
            print_value(&value, json_output);
            Ok(0)
        }
        GoalCommand::Clear(GoalClearArgs { agent, mode }) => {
            let request = GoalClearRequest {
                id: mutation_id(id)?,
                agent,
                mode: goal_mode(&mode)?,
            };
            request.validate().map_err(Failure::for_mutation)?;
            let response: GoalView =
                eliot_daemon::client_call_typed(&data_dir, "goal.clear", &request)
                    .await
                    .map_err(Failure::for_mutation)?;
            request
                .validate_response(&response)
                .map_err(Failure::for_mutation)?;
            let value = serde_json::to_value(response)
                .map_err(|_| Failure::new(4, "goal.clear response is invalid"))?;
            print_value(&value, json_output);
            Ok(0)
        }
        GoalCommand::Resume(GoalResumeArgs { agent, mode }) => {
            let request = GoalResumeRequest {
                id: mutation_id(id)?,
                agent,
                mode: goal_mode(&mode)?,
            };
            request.validate().map_err(Failure::for_mutation)?;
            let response: GoalView =
                eliot_daemon::client_call_typed(&data_dir, "goal.resume", &request)
                    .await
                    .map_err(Failure::for_mutation)?;
            request
                .validate_response(&response)
                .map_err(Failure::for_mutation)?;
            let value = serde_json::to_value(response)
                .map_err(|_| Failure::new(4, "goal.resume response is invalid"))?;
            print_value(&value, json_output);
            Ok(0)
        }
        GoalCommand::Round(GoalRoundArgs {
            agent,
            mode,
            idle_seq,
            slot,
        }) => {
            if idle_seq.is_none() == slot.is_none() {
                return Err(Failure::new(
                    2,
                    "goal round needs exactly one of --idle-seq, --slot",
                ));
            }
            let request = GoalRoundRequest {
                id: mutation_id(id)?,
                agent,
                mode: goal_mode(&mode)?,
                idle_seq,
                slot,
            };
            request.validate().map_err(Failure::for_mutation)?;
            let response: GoalRoundResponse =
                eliot_daemon::client_call_typed(&data_dir, "goal.round", &request)
                    .await
                    .map_err(Failure::for_mutation)?;
            request
                .validate_response(&response)
                .map_err(Failure::for_mutation)?;
            let value = serde_json::to_value(response)
                .map_err(|_| Failure::new(4, "goal.round response is invalid"))?;
            print_value(&value, json_output);
            Ok(0)
        }
    }
}

pub(crate) fn goal_mode(mode: &str) -> Result<GoalMode, Failure> {
    GoalMode::parse(mode).map_err(|_| Failure::new(2, "--mode must be eliot or native"))
}
