//! Owner-state commands: provider, project, rule, timer, goal.
use super::args::{
    GoalArgs, GoalClearArgs, GoalCommand, GoalPauseArgs, GoalResumeArgs, GoalRoundArgs,
    GoalSetArgs, GoalShowArgs, ProjectArgs, ProjectCommand, ProjectShowArgs, ProviderArgs,
    ProviderClearHoldArgs, ProviderCommand, ProviderHoldsArgs, RuleArgs, RuleCatchUpArgs,
    RuleCommand, RuleShowArgs, TimerArgs, TimerCommand, TimerRunArgs, TimerShowArgs,
};
use super::support::read_and_print;
use super::support::{Failure, command_value, mutation_id, print_value, response_exit_code};
use eliot_core::Command as CoreCommand;
use eliot_core::api::{
    DEFAULT_GOAL_CHECK_TIMEOUT_MS, GoalClearRequest, GoalDefinition, GoalGetRequest, GoalMode,
    GoalPauseRequest, GoalResumeRequest, GoalRoundRequest, GoalRoundResponse, GoalSetRequest,
    GoalView, ProjectRegistrationView, ProviderClearHoldRequest, ProviderClearHoldResponse,
    ProviderCorrectiveEvidence, ProviderHoldsRequest, ProviderHoldsResponse, RuleCatchUpRequest,
    RuleGetRequest, RuleStateView, TimerGetRequest, TimerRunRequest, TimerStateView,
};
use serde_json::Value;
use serde_json::json;
use std::path::PathBuf;

pub(crate) async fn run_provider(
    ProviderArgs { command }: ProviderArgs,
    data_dir: PathBuf,
    json_output: bool,
    id: Option<String>,
) -> Result<u8, Failure> {
    match command {
        ProviderCommand::Holds(ProviderHoldsArgs { after, limit }) => {
            let request = ProviderHoldsRequest { after, limit };
            request
                .validate()
                .map_err(|error| Failure::from_core(error, false))?;
            let response: ProviderHoldsResponse =
                eliot_daemon::client_call_typed(&data_dir, "provider.holds", &request)
                    .await
                    .map_err(|error| Failure::from_core(error, false))?;
            request
                .validate_response(&response)
                .map_err(|_| Failure::new(4, "provider.holds response is invalid"))?;
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
            request
                .validate()
                .map_err(|error| Failure::from_core(error, true))?;
            let response: ProviderClearHoldResponse =
                eliot_daemon::client_call_typed(&data_dir, "provider.clear_hold", &request)
                    .await
                    .map_err(|error| Failure::from_core(error, true))?;
            request
                .validate_response(&response)
                .map_err(|_| Failure::new(4, "provider.clear_hold response is invalid"))?;
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
) -> Result<u8, Failure> {
    match command {
        ProjectCommand::List => {
            let response = eliot_daemon::client_call(&data_dir, "project.list", json!({}))
                .await
                .map_err(|error| Failure::from_core(error, false))?;
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
        ProjectCommand::Show(ProjectShowArgs { id }) => {
            let response: ProjectRegistrationView =
                eliot_daemon::client_call_typed(&data_dir, "project.get", &json!({"id":id}))
                    .await
                    .map_err(|error| Failure::from_core(error, false))?;
            if response.id != id || response.validate().is_err() {
                return Err(Failure::new(4, "project.get response is invalid"));
            }
            let value = serde_json::to_value(response)
                .map_err(|_| Failure::new(4, "project.get response is invalid"))?;
            print_value(&value, json_output);
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
        RuleCommand::Show(RuleShowArgs { id, project }) => {
            let request = match project {
                Some(project) => RuleGetRequest::new(id).in_project(project),
                None => RuleGetRequest::new(id),
            };
            read_and_print!(&data_dir, json_output, "rule.get", request, RuleStateView)
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
            request
                .validate()
                .map_err(|error| Failure::from_core(error, true))?;
            let response: CoreCommand =
                eliot_daemon::client_call_typed(&data_dir, "rule.catch_up", &request)
                    .await
                    .map_err(|error| Failure::from_core(error, true))?;
            request
                .validate_response(&response)
                .map_err(|error| Failure::from_core(error, true))?;
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
        TimerCommand::Show(TimerShowArgs { id, project }) => {
            let request = match project {
                Some(project) => TimerGetRequest::new(id).in_project(project),
                None => TimerGetRequest::new(id),
            };
            read_and_print!(&data_dir, json_output, "timer.get", request, TimerStateView)
        }
        TimerCommand::Run(TimerRunArgs { rule, project }) => {
            let request = TimerRunRequest::new(mutation_id(id)?, rule);
            let request = match project {
                Some(project) => request.in_project(project),
                None => request,
            };
            request
                .validate()
                .map_err(|error| Failure::from_core(error, true))?;
            let response: CoreCommand =
                eliot_daemon::client_call_typed(&data_dir, "timer.run", &request)
                    .await
                    .map_err(|error| Failure::from_core(error, true))?;
            request
                .validate_response(&response)
                .map_err(|error| Failure::from_core(error, true))?;
            let exit_code = response_exit_code(response.state);
            let value = command_value(response)?;
            print_value(&value, json_output);
            Ok(exit_code)
        }
    }
}

pub(crate) async fn run_goal(
    GoalArgs { command }: GoalArgs,
    data_dir: PathBuf,
    json_output: bool,
    id: Option<String>,
) -> Result<u8, Failure> {
    match command {
        GoalCommand::Set(GoalSetArgs {
            agent,
            mode,
            text,
            check,
            check_cwd,
            check_timeout_ms,
            max_turns,
            max_rounds,
            max_without_progress,
            cadence_ms,
        }) => {
            let request = GoalSetRequest {
                id: mutation_id(id)?,
                agent,
                mode: goal_mode(&mode)?,
                definition: GoalDefinition {
                    text,
                    check,
                    check_cwd,
                    check_timeout_ms: check_timeout_ms.unwrap_or(DEFAULT_GOAL_CHECK_TIMEOUT_MS),
                    max_turns,
                    max_rounds,
                    max_without_progress,
                    cadence_ms,
                },
                max_cost: None,
            };
            request
                .validate()
                .map_err(|error| Failure::from_core(error, true))?;
            let response: GoalView =
                eliot_daemon::client_call_typed(&data_dir, "goal.set", &request)
                    .await
                    .map_err(|error| Failure::from_core(error, true))?;
            request
                .validate_response(&response)
                .map_err(|error| Failure::from_core(error, true))?;
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
            read_and_print!(&data_dir, json_output, "goal.get", request, GoalView)
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
            request
                .validate()
                .map_err(|error| Failure::from_core(error, true))?;
            let response: GoalView =
                eliot_daemon::client_call_typed(&data_dir, "goal.pause", &request)
                    .await
                    .map_err(|error| Failure::from_core(error, true))?;
            request
                .validate_response(&response)
                .map_err(|error| Failure::from_core(error, true))?;
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
            request
                .validate()
                .map_err(|error| Failure::from_core(error, true))?;
            let response: GoalView =
                eliot_daemon::client_call_typed(&data_dir, "goal.clear", &request)
                    .await
                    .map_err(|error| Failure::from_core(error, true))?;
            request
                .validate_response(&response)
                .map_err(|error| Failure::from_core(error, true))?;
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
            request
                .validate()
                .map_err(|error| Failure::from_core(error, true))?;
            let response: GoalView =
                eliot_daemon::client_call_typed(&data_dir, "goal.resume", &request)
                    .await
                    .map_err(|error| Failure::from_core(error, true))?;
            request
                .validate_response(&response)
                .map_err(|error| Failure::from_core(error, true))?;
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
            request
                .validate()
                .map_err(|error| Failure::from_core(error, true))?;
            let response: GoalRoundResponse =
                eliot_daemon::client_call_typed(&data_dir, "goal.round", &request)
                    .await
                    .map_err(|error| Failure::from_core(error, true))?;
            request
                .validate_response(&response)
                .map_err(|error| Failure::from_core(error, true))?;
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
