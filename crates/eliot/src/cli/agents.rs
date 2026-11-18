//! Agent lifecycle and coordination writes.
use super::args::{
    AnswerArgs, ApproveArgs, AttachArgs, ConfigureArgs, DenyArgs, DetachArgs, DigestArgs,
    DiscoverArgs, GenerationArgs, InjectCliArgs, InterruptArgs, LaunchArgs, LaunchQueueArgs,
    ModelsArgs, PageArgs, QuotaArgs, ReadGenerationArgs, ResumeArgs, SendArgs, SteerArgs, StopArgs,
    WaitArgs, WorktreeArgs, WorktreeCommand, WorktreeRemoveArgs,
};
use super::support::read_and_print;
use super::support::{
    DEFAULT_WAIT_SECONDS, Failure, INTERRUPTED_EXIT_CODE, MAX_ANSWERS_FILE_BYTES, MAX_TEXT_BYTES,
    command_value, mutation_id, print_value, response_exit_code, submit_and_print, submit_command,
    submit_request, submit_value, turn_exit_code, turn_value, validate_limit, wait_deadline,
    wait_for_command, wait_for_linked_turn, wait_for_turn,
};
use eliot_core::api::{
    AgentDiscoverRequest, AgentGetRequest, AgentGetResponse, DiscoverPage, HarnessListRequest,
    HarnessListResponse, HarnessModelsRequest, HarnessModelsResponse, HarnessQuotaRequest,
    HarnessQuotaResponse, MAX_SWARM_WAIT_TARGETS, SwarmDigestRequest, SwarmDigestResponse,
    SwarmWaitRequest, SwarmWaitResponse, WaitTurnRef,
};
use eliot_core::{InjectArgs, InjectPoint, SubmitCommand};
use serde_json::Value;
use serde_json::json;
use std::{
    fs::File,
    io::Read,
    path::{Path, PathBuf},
};

pub(crate) async fn run_discover(
    DiscoverArgs {
        profile,
        harness,
        scope,
        cursor,
        archived,
        page: PageArgs { limit },
    }: DiscoverArgs,
    data_dir: PathBuf,
    json_output: bool,
) -> Result<u8, Failure> {
    validate_limit(limit)?;
    let params = AgentDiscoverRequest {
        profile,
        harness,
        native_scope: scope,
        cursor,
        archived,
        limit,
    };
    read_and_print!(
        &data_dir,
        json_output,
        "agent.discover",
        params,
        DiscoverPage
    )
}

pub(crate) async fn run_harnesses(data_dir: PathBuf, json_output: bool) -> Result<u8, Failure> {
    read_and_print!(
        &data_dir,
        json_output,
        "harness.list",
        HarnessListRequest {},
        HarnessListResponse
    )
}

pub(crate) async fn run_models(
    ModelsArgs {
        harness,
        profile,
        agent,
        workspace,
        query,
        after,
        limit,
    }: ModelsArgs,
    data_dir: PathBuf,
    json_output: bool,
) -> Result<u8, Failure> {
    use eliot_core::api::{ModelsCatalogRequest, ModelsCatalogResponse};
    if let Some(harness) = harness {
        if harness.trim().is_empty() {
            return Err(Failure::new(2, "--harness must not be empty"));
        }
        if profile
            .as_deref()
            .is_some_and(|profile| profile.trim().is_empty())
        {
            return Err(Failure::new(2, "--profile must not be empty"));
        }
        if query.is_some() || after.is_some() || limit.is_some() {
            return Err(Failure::new(
                2,
                "--query/--after/--limit require an omitted --harness (unified catalogue)",
            ));
        }
        read_and_print!(
            &data_dir,
            json_output,
            "harness.models",
            HarnessModelsRequest {
                harness,
                profile,
                agent,
                workspace,
            },
            HarnessModelsResponse
        )
    } else {
        if profile.is_some() {
            return Err(Failure::new(2, "--profile requires --harness"));
        }
        if query
            .as_deref()
            .is_some_and(|query| query.trim().is_empty())
        {
            return Err(Failure::new(2, "--query must not be empty"));
        }
        if after
            .as_deref()
            .is_some_and(|after| after.trim().is_empty())
        {
            return Err(Failure::new(2, "--after must not be empty"));
        }
        if limit.is_some_and(|limit| limit == 0 || limit > 100) {
            return Err(Failure::new(2, "--limit must be from 1 through 100"));
        }
        read_and_print!(
            &data_dir,
            json_output,
            "models.catalog",
            ModelsCatalogRequest {
                query,
                after,
                limit,
            },
            ModelsCatalogResponse
        )
    }
}

pub(crate) async fn run_quota(
    QuotaArgs { harness, profile }: QuotaArgs,
    data_dir: PathBuf,
    json_output: bool,
) -> Result<u8, Failure> {
    if harness.trim().is_empty() {
        return Err(Failure::new(2, "--harness must not be empty"));
    }
    if profile
        .as_deref()
        .is_some_and(|profile| profile.trim().is_empty())
    {
        return Err(Failure::new(2, "--profile must not be empty"));
    }
    read_and_print!(
        &data_dir,
        json_output,
        "harness.quota",
        HarnessQuotaRequest { harness, profile },
        HarnessQuotaResponse
    )
}

pub(crate) async fn run_attach(
    AttachArgs {
        profile,
        harness,
        scope,
        native_ref,
        project,
    }: AttachArgs,
    data_dir: PathBuf,
    json_output: bool,
    id: Option<String>,
) -> Result<u8, Failure> {
    let mut args = json!({
        "mode": "observe",
        "profile": profile,
        "harness": harness,
        "native_scope": scope,
        "native_ref": native_ref,
    });
    if let Some(project) = project {
        args["project"] = Value::String(project);
    }
    let params = submit_request(id, None, "attach", args)?;
    submit_and_print(&data_dir, &params, json_output).await
}

pub(crate) async fn run_launch(
    LaunchArgs {
        profile,
        role,
        harness,
        model,
        effort,
        auth,
        access,
        cmd,
        workspace,
        chat,
        worktree,
        base,
        parent,
        prompt,
        queue,
        owner_override,
        wait,
        timeout_seconds,
    }: LaunchArgs,
    data_dir: PathBuf,
    json_output: bool,
    id: Option<String>,
) -> Result<u8, Failure> {
    if let Some(name) = &profile
        && name.is_empty()
    {
        return Err(Failure::new(2, "--profile must not be empty"));
    }
    if let Some(name) = &role
        && (name.trim().is_empty()
            || name.len() > 128
            || !name.chars().all(|character| {
                character.is_ascii_alphanumeric() || matches!(character, '-' | '_')
            }))
    {
        return Err(Failure::new(
            2,
            "--role must be a nonempty role name using letters, digits, '-' or '_'",
        ));
    }
    let workspace = workspace
        .map(std::path::absolute)
        .transpose()
        .map_err(|error| Failure::new(2, format!("cannot resolve launch workspace: {error}")))?;
    if workspace.is_none()
        && !chat
        && parent.is_none()
        && std::env::var_os("ELIOT_AGENT_ID").is_none()
    {
        return Err(Failure::new(2, "launch requires --workspace or --chat"));
    }
    if let Some(name) = &harness
        && name.trim().is_empty()
    {
        return Err(Failure::new(2, "--harness must not be empty"));
    }
    if harness.is_some() && parent.is_some() {
        return Err(Failure::new(2, "ad-hoc launch cannot have a parent"));
    }
    if harness.is_none() {
        if model.is_some()
            || effort.is_some()
            || auth.is_some()
            || access.is_some()
            || cmd.is_some()
        {
            return Err(Failure::new(
                2,
                "--model/--effort/--auth/--access/--cmd require --harness",
            ));
        }
    } else if let Some(name) = &model
        && name.trim().is_empty()
    {
        return Err(Failure::new(2, "--model must not be empty"));
    } else if let Some(name) = &effort
        && name.trim().is_empty()
    {
        return Err(Failure::new(2, "--effort must not be empty"));
    } else if let Some(name) = &auth
        && name.trim().is_empty()
    {
        return Err(Failure::new(2, "--auth must not be empty"));
    } else if let Some(name) = &access
        && name.trim().is_empty()
    {
        return Err(Failure::new(2, "--access must not be empty"));
    } else if let Some(name) = &cmd
        && name.trim().is_empty()
    {
        return Err(Failure::new(2, "--cmd must not be empty"));
    }
    validate_text(prompt.as_deref())?;
    let deadline = wait_deadline(timeout_seconds)?;
    let mut args = json!({});
    if let Some(workspace) = workspace {
        args["workspace"] = json!(workspace.to_string_lossy());
    }
    if chat {
        args["chat"] = json!(true);
    }
    if let Some(profile) = profile {
        args["profile"] = Value::String(profile);
    }
    if let Some(role) = role {
        args["role"] = Value::String(role);
    }
    if let Some(harness) = harness {
        args["harness"] = Value::String(harness);
    }
    if let Some(model) = model {
        args["model"] = Value::String(model);
    }
    if let Some(effort) = effort {
        args["effort"] = Value::String(effort);
    }
    if let Some(auth) = auth {
        args["auth"] = Value::String(auth);
    }
    if let Some(access) = access {
        args["access"] = Value::String(access);
    }
    if let Some(cmd) = cmd {
        args["cmd"] = Value::String(cmd);
    }
    if let Some(worktree) = worktree {
        args["worktree"] = Value::String(worktree);
    }
    if let Some(base) = base {
        args["base"] = Value::String(base);
    }
    if let Some(parent) = parent {
        args["parent"] = Value::String(parent);
    }
    if let Some(prompt) = &prompt {
        args["text"] = Value::String(prompt.clone());
    }
    if queue {
        let request = eliot_core::api::LaunchQueueRequest {
            id: mutation_id(id)?,
            args,
        };
        request.validate().map_err(Failure::for_mutation)?;
        let receipt: eliot_core::api::LaunchIntentView =
            eliot_daemon::client_call_typed(&data_dir, "launch.enqueue", &request)
                .await
                .map_err(Failure::for_mutation)?;
        if receipt.id != request.id {
            return Err(Failure::new(4, "launch.enqueue response id differs"));
        }
        let rejected = receipt.state == eliot_core::api::LaunchIntentState::Rejected;
        print_value(&json!(receipt), json_output);
        return Ok(u8::from(rejected));
    }
    if owner_override {
        args["owner_override"] = json!(true);
    }
    let params = submit_request(id, None, "launch", args)?;
    let submitted = submit_command(&data_dir, &params).await?;
    let submit_exit = response_exit_code(submitted.state);
    if matches!(submit_exit, 1 | 3 | 4) {
        print_value(&submit_value(&submitted)?, json_output);
        return Ok(submit_exit);
    }

    let root_id = &submitted.command_id;
    if !wait {
        let command = wait_for_command(&data_dir, root_id, deadline).await?;
        let exit_code = response_exit_code(command.state);
        print_value(&command_value(command)?, json_output);
        return Ok(exit_code);
    }

    let (completed, exit_code) = if prompt.is_some() {
        let linked = submitted
            .linked_command
            .as_deref()
            .ok_or_else(|| Failure::new(4, "launch response omitted its linked input command"))?;
        let turn = wait_for_linked_turn(&data_dir, root_id, linked, deadline).await?;
        let exit_code = turn_exit_code(&turn);
        (turn_value(turn)?, exit_code)
    } else {
        // Promptless launch carries no turn to judge: readiness is
        // the root command applying, not a turn outcome.
        let command = wait_for_command(&data_dir, root_id, deadline).await?;
        let exit_code = response_exit_code(command.state);
        (command_value(command)?, exit_code)
    };
    print_value(&completed, json_output);
    Ok(exit_code)
}

pub(crate) async fn run_launch_queue(
    LaunchQueueArgs {
        intent,
        project,
        after,
    }: LaunchQueueArgs,
    data_dir: PathBuf,
    json_output: bool,
) -> Result<u8, Failure> {
    let value = if let Some(id) = intent {
        let request = eliot_core::api::LaunchIntentGetRequest {
            id: id.to_ascii_uppercase(),
        };
        request.validate().map_err(Failure::for_read)?;
        let reply: eliot_core::api::LaunchIntentView =
            eliot_daemon::client_call_typed(&data_dir, "launch.intent.get", &request)
                .await
                .map_err(Failure::for_read)?;
        if reply.id != request.id {
            return Err(Failure::new(4, "launch.intent.get response id differs"));
        }
        json!(reply)
    } else {
        let request = eliot_core::api::LaunchQueueListRequest { project, after };
        request.validate().map_err(Failure::for_read)?;
        let reply: eliot_core::api::LaunchQueueView =
            eliot_daemon::client_call_typed(&data_dir, "launch.queue", &request)
                .await
                .map_err(Failure::for_read)?;
        json!(reply)
    };
    print_value(&value, json_output);
    Ok(0)
}

pub(crate) async fn run_send(
    SendArgs {
        agent,
        text,
        wait,
        probe,
        generation: GenerationArgs { generation },
        timeout_seconds,
    }: SendArgs,
    data_dir: PathBuf,
    json_output: bool,
    id: Option<String>,
) -> Result<u8, Failure> {
    validate_text(Some(&text))?;
    let deadline = if wait {
        Some(wait_deadline(timeout_seconds)?)
    } else {
        None
    };
    let mut params = submit_request(id, Some(agent), "send", json!({ "text": text }))?;
    params.expected_generation = generation;
    params.probe = probe;
    let submitted = submit_command(&data_dir, &params).await?;
    let submit_exit = response_exit_code(submitted.state);
    if matches!(submit_exit, 1 | 3 | 4) {
        print_value(&submit_value(&submitted)?, json_output);
        return Ok(submit_exit);
    }
    if !wait {
        print_value(&submit_value(&submitted)?, json_output);
        return Ok(submit_exit);
    }
    let deadline = deadline.ok_or_else(|| Failure::new(2, "missing wait deadline"))?;
    let completed = wait_for_turn(&data_dir, &submitted.command_id, deadline).await?;
    let exit_code = turn_exit_code(&completed);
    print_value(&turn_value(completed)?, json_output);
    Ok(exit_code)
}

pub(crate) async fn run_resume(
    ResumeArgs {
        agent,
        prompt,
        wait,
        generation: GenerationArgs { generation },
        timeout_seconds,
    }: ResumeArgs,
    data_dir: PathBuf,
    json_output: bool,
    id: Option<String>,
) -> Result<u8, Failure> {
    validate_text(prompt.as_deref())?;
    let deadline = if wait {
        Some(wait_deadline(timeout_seconds)?)
    } else {
        None
    };
    let mut args = json!({});
    if let Some(prompt) = &prompt {
        args["text"] = Value::String(prompt.clone());
    }
    let mut params = submit_request(id, Some(agent), "resume", args)?;
    params.expected_generation = generation;
    let submitted = submit_command(&data_dir, &params).await?;
    let submit_exit = response_exit_code(submitted.state);
    if matches!(submit_exit, 1 | 3 | 4) {
        print_value(&submit_value(&submitted)?, json_output);
        return Ok(submit_exit);
    }
    if !wait {
        print_value(&submit_value(&submitted)?, json_output);
        return Ok(submit_exit);
    }

    let deadline = deadline.ok_or_else(|| Failure::new(2, "missing wait deadline"))?;
    let (completed, exit_code) = if prompt.is_some() {
        let linked = submitted
            .linked_command
            .as_deref()
            .ok_or_else(|| Failure::new(4, "resume response omitted its linked input command"))?;
        let turn = wait_for_linked_turn(&data_dir, &submitted.command_id, linked, deadline).await?;
        let exit_code = turn_exit_code(&turn);
        (turn_value(turn)?, exit_code)
    } else {
        // Promptless resume carries no turn to judge: readiness is
        // the root command applying, not a turn outcome.
        let command = wait_for_command(&data_dir, &submitted.command_id, deadline).await?;
        let exit_code = response_exit_code(command.state);
        (command_value(command)?, exit_code)
    };
    print_value(&completed, json_output);
    Ok(exit_code)
}

pub(crate) async fn run_steer(
    SteerArgs {
        agent,
        text,
        generation: GenerationArgs { generation },
        turn,
    }: SteerArgs,
    data_dir: PathBuf,
    json_output: bool,
    id: Option<String>,
) -> Result<u8, Failure> {
    validate_text(Some(&text))?;
    let (generation, turn, capture) =
        resolve_exact_turn_target(&data_dir, "steer", &agent, generation, turn).await?;
    let mut params = submit_request(id, Some(agent), "steer", json!({ "text": text }))?;
    params.expected_generation = generation;
    params.expected_turn_id = turn;
    submit_exact_turn_command(&data_dir, &params, json_output, capture.as_ref()).await
}

pub(crate) async fn run_inject(
    InjectCliArgs {
        agent,
        text,
        point,
        generation: GenerationArgs { generation },
        turn,
        expires_at_ms,
    }: InjectCliArgs,
    data_dir: PathBuf,
    json_output: bool,
    id: Option<String>,
) -> Result<u8, Failure> {
    validate_text(Some(&text))?;
    let point = match point.as_str() {
        "next_input" => InjectPoint::NextInput,
        "turn_safe_point" => InjectPoint::TurnSafePoint,
        "active_turn" => InjectPoint::ActiveTurn,
        _ => {
            return Err(Failure::new(
                2,
                "inject point must be next_input, turn_safe_point, or active_turn",
            ));
        }
    };
    let args = serde_json::to_value(&InjectArgs {
        text,
        point,
        expires_at_ms,
    })
    .map_err(|_| Failure::new(2, "inject args could not be encoded"))?;
    let mut params = submit_request(id, Some(agent), "inject", args)?;
    params.expected_generation = generation;
    params.expected_turn_id = turn;
    submit_and_print(&data_dir, &params, json_output).await
}

pub(crate) async fn run_interrupt(
    InterruptArgs {
        agent,
        generation: GenerationArgs { generation },
        turn,
    }: InterruptArgs,
    data_dir: PathBuf,
    json_output: bool,
    id: Option<String>,
) -> Result<u8, Failure> {
    let (generation, turn, capture) =
        resolve_exact_turn_target(&data_dir, "interrupt", &agent, generation, turn).await?;
    let mut params = submit_request(id, Some(agent), "interrupt", json!({}))?;
    params.expected_generation = generation;
    params.expected_turn_id = turn;
    submit_exact_turn_command(&data_dir, &params, json_output, capture.as_ref()).await
}

pub(crate) async fn run_approve(
    ApproveArgs {
        agent,
        generation: GenerationArgs { generation },
        request,
        choice,
        feedback,
    }: ApproveArgs,
    data_dir: PathBuf,
    json_output: bool,
    id: Option<String>,
) -> Result<u8, Failure> {
    let mut params = submit_request(
        id,
        Some(agent),
        "approve",
        approval_reply_args(choice, feedback),
    )?;
    params.expected_generation = generation;
    params.request_id = request;
    submit_and_print(&data_dir, &params, json_output).await
}

pub(crate) async fn run_deny(
    DenyArgs {
        agent,
        generation: GenerationArgs { generation },
        request,
        choice,
        feedback,
    }: DenyArgs,
    data_dir: PathBuf,
    json_output: bool,
    id: Option<String>,
) -> Result<u8, Failure> {
    let mut params = submit_request(
        id,
        Some(agent),
        "deny",
        approval_reply_args(choice, feedback),
    )?;
    params.expected_generation = generation;
    params.request_id = request;
    submit_and_print(&data_dir, &params, json_output).await
}

pub(crate) async fn run_answer(
    AnswerArgs {
        agent,
        answers_file,
        generation: GenerationArgs { generation },
        request,
    }: AnswerArgs,
    data_dir: PathBuf,
    json_output: bool,
    id: Option<String>,
) -> Result<u8, Failure> {
    let answers = read_answers_file(&answers_file)?;
    let mut params = submit_request(id, Some(agent), "answer", json!({ "answers": answers }))?;
    params.expected_generation = generation;
    params.request_id = request;
    submit_and_print(&data_dir, &params, json_output).await
}

pub(crate) async fn run_detach(
    DetachArgs { agent, generation }: DetachArgs,
    data_dir: PathBuf,
    json_output: bool,
    id: Option<String>,
) -> Result<u8, Failure> {
    let mut params = submit_request(id, Some(agent), "detach", json!({}))?;
    params.expected_generation = generation;
    submit_and_print(&data_dir, &params, json_output).await
}

pub(crate) async fn run_stop(
    StopArgs {
        agent,
        force,
        tree,
        generation: GenerationArgs { generation },
    }: StopArgs,
    data_dir: PathBuf,
    json_output: bool,
    id: Option<String>,
) -> Result<u8, Failure> {
    let mut params = submit_request(id, Some(agent), "stop", json!({ "force": force }))?;
    if tree {
        params.selector = params.target.take().map(|agent| format!("tree:{agent}"));
    } else {
        params.expected_generation = generation;
    }
    submit_and_print(&data_dir, &params, json_output).await
}

pub(crate) async fn run_configure(
    ConfigureArgs {
        agent,
        model,
        effort,
        generation: GenerationArgs { generation },
    }: ConfigureArgs,
    data_dir: PathBuf,
    json_output: bool,
    id: Option<String>,
) -> Result<u8, Failure> {
    if model.is_none() && effort.is_none() {
        return Err(Failure::new(
            2,
            "configure requires --model and/or --effort",
        ));
    }
    let mut args = serde_json::Map::new();
    if let Some(model) = model {
        args.insert("model".to_owned(), Value::String(model));
    }
    if let Some(effort) = effort {
        args.insert("effort".to_owned(), Value::String(effort));
    }
    let mut params = submit_request(id, Some(agent), "configure", Value::Object(args))?;
    params.expected_generation = generation;
    submit_and_print(&data_dir, &params, json_output).await
}

pub(crate) async fn run_worktree(
    WorktreeArgs { command }: WorktreeArgs,
    data_dir: PathBuf,
    json_output: bool,
    id: Option<String>,
) -> Result<u8, Failure> {
    match command {
        WorktreeCommand::Remove(WorktreeRemoveArgs {
            agent,
            generation: ReadGenerationArgs { generation },
        }) => {
            let deadline = wait_deadline(DEFAULT_WAIT_SECONDS)?;
            let mut params = submit_request(id, Some(agent), "worktree.remove", json!({}))?;
            params.expected_generation = generation;
            let submitted = submit_command(&data_dir, &params).await?;
            let submit_exit = response_exit_code(submitted.state);
            if matches!(submit_exit, 1 | 3 | 4) {
                print_value(&submit_value(&submitted)?, json_output);
                return Ok(submit_exit);
            }
            let completed = wait_for_command(&data_dir, &submitted.command_id, deadline).await?;
            let exit_code = response_exit_code(completed.state);
            print_value(&command_value(completed)?, json_output);
            Ok(exit_code)
        }
    }
}

pub(crate) async fn run_wait(
    WaitArgs {
        all,
        any,
        timeout_seconds,
        ids,
        command,
        turn,
        mail,
    }: WaitArgs,
    data_dir: PathBuf,
    json_output: bool,
) -> Result<u8, Failure> {
    swarm_wait(
        &data_dir,
        all,
        any,
        timeout_seconds,
        ids,
        command,
        turn,
        mail,
        json_output,
    )
    .await
}

pub(crate) async fn run_digest(
    DigestArgs {
        all,
        any,
        to,
        ids,
        command,
        turn,
    }: DigestArgs,
    data_dir: PathBuf,
    json_output: bool,
    id: Option<String>,
) -> Result<u8, Failure> {
    swarm_digest(&data_dir, all, any, to, ids, command, turn, json_output, id).await
}

pub(crate) async fn run_reparent(
    args: super::args::ReparentArgs,
    data_dir: PathBuf,
    json_output: bool,
    id: Option<String>,
) -> Result<u8, Failure> {
    let request = eliot_core::api::ReparentRequest {
        id: mutation_id(id)?,
        agent: args.agent,
        children_of: args.children_of,
        expected_generation: args.generation,
        parent: args.parent,
        parent_generation: args.parent_generation,
    };
    request.validate().map_err(Failure::for_mutation)?;
    let response: eliot_core::api::ReparentResponse =
        eliot_daemon::client_call_typed(&data_dir, "swarm.reparent", &request)
            .await
            .map_err(Failure::for_mutation)?;
    if response.id != request.id {
        return Err(Failure::new(4, "swarm.reparent response id differs"));
    }
    print_value(
        &serde_json::to_value(response)
            .map_err(|_| Failure::new(4, "invalid reparent response"))?,
        json_output,
    );
    Ok(0)
}

pub(crate) async fn run_switch(
    args: super::args::SwitchArgs,
    data_dir: PathBuf,
    json_output: bool,
    id: Option<String>,
) -> Result<u8, Failure> {
    let request = eliot_core::api::SwitchRequest {
        id: mutation_id(id)?,
        from: args.from,
        from_generation: args.generation,
        task_command: args.task_command,
        commands: args.commands,
        launch: eliot_core::api::SwitchLaunch {
            profile: args.profile,
            harness: args.harness,
            model: args.model,
            effort: args.effort,
            auth: args.auth,
            cmd: args.cmd,
            access: args.access,
            workspace: args.workspace,
            worktree: args.worktree,
            base: args.base,
        },
    };
    request.validate().map_err(Failure::for_mutation)?;
    let response: eliot_core::api::SwitchResponse =
        eliot_daemon::client_call_typed(&data_dir, "swarm.switch", &request)
            .await
            .map_err(Failure::for_mutation)?;
    request
        .validate_response(&response)
        .map_err(Failure::invalid_response)?;
    let exit = response_exit_code(response.state);
    print_value(
        &serde_json::to_value(response).map_err(|_| Failure::new(4, "invalid switch response"))?,
        json_output,
    );
    Ok(exit)
}

pub(crate) async fn run_handoff(
    args: super::args::HandoffArgs,
    data_dir: PathBuf,
    json_output: bool,
    id: Option<String>,
) -> Result<u8, Failure> {
    let request = eliot_core::api::HandoffRequest {
        id: mutation_id(id)?,
        from: args.from,
        from_generation: args.generation,
        to: args.to,
        to_generation: args.to_generation,
        task_command: args.task_command,
        commands: args.commands,
    };
    request.validate().map_err(Failure::for_mutation)?;
    let response: eliot_core::api::HandoffResponse =
        eliot_daemon::client_call_typed(&data_dir, "swarm.handoff", &request)
            .await
            .map_err(Failure::for_mutation)?;
    request
        .validate_response(&response)
        .map_err(Failure::invalid_response)?;
    print_value(
        &serde_json::to_value(response).map_err(|_| Failure::new(4, "invalid handoff response"))?,
        json_output,
    );
    Ok(0)
}

pub(crate) fn approval_reply_args(choice: Option<String>, feedback: Option<String>) -> Value {
    let mut args = serde_json::Map::new();
    if let Some(choice) = choice {
        args.insert("choice_id".to_owned(), Value::String(choice));
    }
    if let Some(feedback) = feedback {
        args.insert("feedback".to_owned(), Value::String(feedback));
    }
    Value::Object(args)
}

/// An exact turn binding the CLI captured from the Agent's current generation
/// and active turn for an omitted `--generation`/`--turn` flag. The captured
/// identity is submitted unchanged; a later rejection means the turn moved.
pub(crate) struct ExactTurnCapture {
    verb: &'static str,
    agent: String,
    generation: i64,
    turn_id: String,
}

impl ExactTurnCapture {
    /// A captured submit rejected as `stale_generation` lost a race: the
    /// generation or active turn changed between the capture read and
    /// admission. The command was rejected before any effect, so the CLI
    /// reports the race and never retries against the new turn.
    pub(crate) fn race(&self) -> Failure {
        Failure::new(
            3,
            format!(
                "{} race: Agent {} turn changed after capture \
                 (submitted generation={} turn={}); \
                 rejected before any effect, not retried",
                self.verb, self.agent, self.generation, self.turn_id
            ),
        )
    }
}

pub(crate) fn missing_turn_failure(verb: &str, agent: &str) -> Failure {
    Failure::new(
        3,
        format!("{verb} requires an active turn: Agent {agent} has no active turn"),
    )
}

/// A `stale_generation` rejection of a captured exact-target submit is a race,
/// not a generic rejection. Every other failure passes through unchanged.
pub(crate) fn race_or_failure(capture: Option<&ExactTurnCapture>, failure: Failure) -> Failure {
    match capture {
        Some(capture) if failure.code.as_deref() == Some("stale_generation") => {
            let mut race = capture.race();
            race.diagnostics = failure.diagnostics;
            race
        }
        _ => failure,
    }
}

/// Read the Agent's current generation and active turn for an omitted
/// `--generation`/`--turn` flag. Fails clearly when there is no active turn.
pub(crate) async fn capture_exact_turn(
    data_dir: &Path,
    verb: &'static str,
    agent: &str,
) -> Result<ExactTurnCapture, Failure> {
    let params = AgentGetRequest::new(agent);
    params.validate().map_err(Failure::for_read)?;
    let response: AgentGetResponse =
        eliot_daemon::client_call_typed(data_dir, "agent.get", &params)
            .await
            .map_err(Failure::for_read)?;
    params
        .validate_response(&response)
        .map_err(Failure::invalid_response)?;
    let turn_id = response
        .agent
        .active_turn
        .clone()
        .ok_or_else(|| missing_turn_failure(verb, agent))?;
    Ok(ExactTurnCapture {
        verb,
        agent: agent.to_owned(),
        generation: response.agent.generation,
        turn_id,
    })
}

/// Resolve the exact binding a turn-control command submits. Explicit flags
/// are kept unchanged with no extra read; each omitted flag is filled from a
/// single capture read, which is printed before submitting.
pub(crate) async fn resolve_exact_turn_target(
    data_dir: &Path,
    verb: &'static str,
    agent: &str,
    generation: Option<i64>,
    turn: Option<String>,
) -> Result<(Option<i64>, Option<String>, Option<ExactTurnCapture>), Failure> {
    if generation.is_some() && turn.is_some() {
        return Ok((generation, turn, None));
    }
    let capture = capture_exact_turn(data_dir, verb, agent).await?;
    let resolved_generation = generation.unwrap_or(capture.generation);
    let resolved_turn = turn.unwrap_or_else(|| capture.turn_id.clone());
    eprintln!("{verb} target: agent={agent} generation={resolved_generation} turn={resolved_turn}");
    let submitted = ExactTurnCapture {
        verb,
        agent: agent.to_owned(),
        generation: resolved_generation,
        turn_id: resolved_turn.clone(),
    };
    Ok((
        Some(resolved_generation),
        Some(resolved_turn),
        Some(submitted),
    ))
}

pub(crate) async fn submit_exact_turn_command(
    data_dir: &Path,
    params: &SubmitCommand,
    json_output: bool,
    capture: Option<&ExactTurnCapture>,
) -> Result<u8, Failure> {
    match submit_command(data_dir, params).await {
        Ok(submitted) => {
            print_value(&submit_value(&submitted)?, json_output);
            Ok(response_exit_code(submitted.state))
        }
        Err(failure) => Err(race_or_failure(capture, failure)),
    }
}

pub(crate) fn read_answers_file(path: &Path) -> Result<Value, Failure> {
    let file = File::open(path).map_err(|_| Failure::new(2, "answers file could not be read"))?;
    let mut bounded = file.take(MAX_ANSWERS_FILE_BYTES as u64 + 1);
    let mut bytes = Vec::new();
    let byte_count = bounded
        .read_to_end(&mut bytes)
        .map_err(|_| Failure::new(2, "answers file could not be read"))?;
    if byte_count > MAX_ANSWERS_FILE_BYTES {
        return Err(Failure::new(2, "answers file exceeds the 512 KiB limit"));
    }

    let answers: Value = serde_json::from_slice(&bytes)
        .map_err(|_| Failure::new(2, "answers file must contain valid answer JSON"))?;
    if !answers.is_object() {
        return Err(Failure::new(
            2,
            "answers file must be an object keyed by question id",
        ));
    }
    eliot_core::requests::parse_answers_map(&answers).map_err(|_| {
        Failure::new(
            2,
            "each answer entry must contain only an answers array of strings",
        )
    })?;
    Ok(answers)
}

pub(crate) fn validate_text(text: Option<&str>) -> Result<(), Failure> {
    if let Some(text) = text.filter(|text| text.len() > MAX_TEXT_BYTES) {
        return Err(Failure::new(
            2,
            format!(
                "input is {} bytes; the maximum is {MAX_TEXT_BYTES}",
                text.len()
            ),
        ));
    }
    Ok(())
}

/// Bounded wait over a frozen set of child commands/turns. Positional ids are
/// command ids, or `agent/generation/turn_id` for exact turn bindings. The set
/// is frozen client-side before the RPC; the daemon never adds later turns or
/// children. Exit 0 only when every entry succeeded (`--all`) or at least one
/// succeeded (`--any`); timeout exits 5 with the honest per-entry snapshot.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn swarm_wait(
    data_dir: &Path,
    all: bool,
    any: bool,
    timeout_seconds: u64,
    ids: Vec<String>,
    explicit_commands: Vec<String>,
    explicit_turns: Vec<String>,
    mail: bool,
    json_output: bool,
) -> Result<u8, Failure> {
    if all == any {
        return Err(Failure::new(
            2,
            "swarm wait requires exactly one of --all or --any",
        ));
    }
    if timeout_seconds == 0 || timeout_seconds > 30 {
        return Err(Failure::new(
            2,
            "--timeout-seconds must be between 1 and 30",
        ));
    }
    if ids.is_empty() || ids.len() > MAX_SWARM_WAIT_TARGETS {
        return Err(Failure::new(2, "swarm wait requires 1..=256 captured ids"));
    }
    let mode = if all { "all" } else { "any" };
    let mut commands = Vec::new();
    let mut turns = Vec::new();
    for id in ids {
        match positional_wait_target("swarm wait", &id)? {
            WaitTarget::Command(command) => commands.push(command),
            WaitTarget::Turn(turn) => turns.push(turn),
        }
    }
    explicit_wait_targets(
        "swarm wait",
        &mut commands,
        &mut turns,
        &explicit_commands,
        &explicit_turns,
    )?;
    if commands.is_empty() && turns.is_empty() {
        return Err(Failure::new(2, "swarm wait requires 1..=256 captured ids"));
    }
    let wait_ms = timeout_seconds.saturating_mul(1_000);
    let params = SwarmWaitRequest::new(commands, turns, mode, wait_ms).with_mailbox(mail);
    params.validate().map_err(Failure::for_read)?;
    let response: SwarmWaitResponse =
        eliot_daemon::client_call_typed(data_dir, "swarm.wait", &params)
            .await
            .map_err(Failure::for_read)?;
    params
        .validate_response(&response)
        .map_err(Failure::invalid_response)?;
    let value = serde_json::to_value(&response)
        .map_err(|_| Failure::new(4, "swarm.wait response is invalid"))?;
    print_value(&value, json_output);
    Ok(swarm_wait_exit_code(&response))
}

/// Durable completion digest over a frozen set of child commands/turns.
/// Registers one parent mailbox recipient; the daemon delivers exactly one
/// coalesced mail when the set satisfies `all`/`any` and wakes through the
/// standard mail notice. Reusing `--id` replays the same digest without a
/// second mail. Exit 0 when the digest is accepted (pending or delivered).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn swarm_digest(
    data_dir: &Path,
    all: bool,
    any: bool,
    to: String,
    ids: Vec<String>,
    explicit_commands: Vec<String>,
    explicit_turns: Vec<String>,
    json_output: bool,
    id: Option<String>,
) -> Result<u8, Failure> {
    if all == any {
        return Err(Failure::new(
            2,
            "swarm digest requires exactly one of --all or --any",
        ));
    }
    if to.trim().is_empty() {
        return Err(Failure::new(2, "swarm digest requires --to AGENT"));
    }
    if ids.is_empty() || ids.len() > MAX_SWARM_WAIT_TARGETS {
        return Err(Failure::new(
            2,
            "swarm digest requires 1..=256 captured ids",
        ));
    }
    let mode = if all { "all" } else { "any" };
    let mut commands = Vec::new();
    let mut turns = Vec::new();
    for id in ids {
        match positional_wait_target("swarm digest", &id)? {
            WaitTarget::Command(command) => commands.push(command),
            WaitTarget::Turn(turn) => turns.push(turn),
        }
    }
    explicit_wait_targets(
        "swarm digest",
        &mut commands,
        &mut turns,
        &explicit_commands,
        &explicit_turns,
    )?;
    if commands.is_empty() && turns.is_empty() {
        return Err(Failure::new(
            2,
            "swarm digest requires 1..=256 captured ids",
        ));
    }
    let params = SwarmDigestRequest::new(mutation_id(id)?, to, mode, commands, turns);
    params.validate().map_err(Failure::for_mutation)?;
    let response: SwarmDigestResponse =
        eliot_daemon::client_call_typed(data_dir, "swarm.digest", &params)
            .await
            .map_err(Failure::for_mutation)?;
    params
        .validate_response(&response)
        .map_err(Failure::invalid_response)?;
    let value = serde_json::to_value(&response)
        .map_err(|_| Failure::new(4, "swarm.digest response is invalid"))?;
    print_value(&value, json_output);
    Ok(0)
}

/// One `swarm wait` / `swarm digest` target: an opaque command id or an exact
/// turn binding.
pub(crate) enum WaitTarget {
    Command(String),
    Turn(WaitTurnRef),
}

/// The historical positional rule, unchanged: an id containing `/` parses as
/// an exact turn ref, anything else is an opaque command preserved verbatim
/// (including `cmd:legacy`). `cli` names the caller for usage errors
/// (`swarm wait` / `swarm digest`).
pub(crate) fn positional_wait_target(cli: &str, id: &str) -> Result<WaitTarget, Failure> {
    if id.contains('/') {
        parse_wait_turn_ref(id).map(WaitTarget::Turn)
    } else if id.trim().is_empty() {
        Err(Failure::new(
            2,
            format!("{cli} ids must be non-empty strings"),
        ))
    } else {
        Ok(WaitTarget::Command(id.to_owned()))
    }
}

/// Explicit `--command` / `--turn` disambiguation after the positional ids:
/// commands are preserved verbatim (use for ids containing `/` such as
/// linked `<id>/send` and nested derived ids, which positionals reject or
/// misread); turns keep the strict `agent/generation/turn_id` shape.
pub(crate) fn explicit_wait_targets(
    cli: &str,
    commands: &mut Vec<String>,
    turns: &mut Vec<WaitTurnRef>,
    explicit_commands: &[String],
    explicit_turns: &[String],
) -> Result<(), Failure> {
    for command in explicit_commands {
        if command.trim().is_empty() {
            return Err(Failure::new(
                2,
                format!("{cli} --command ids must be non-empty strings"),
            ));
        }
        commands.push(command.clone());
    }
    for turn in explicit_turns {
        turns.push(parse_wait_turn_ref(turn)?);
    }
    Ok(())
}

pub(crate) fn parse_wait_turn_ref(id: &str) -> Result<WaitTurnRef, Failure> {
    let mut parts = id.splitn(3, '/');
    let agent = parts.next().unwrap_or_default();
    let generation = parts.next().unwrap_or_default();
    let turn_id = parts.next().unwrap_or_default();
    if agent.trim().is_empty() || turn_id.trim().is_empty() || generation.trim().is_empty() {
        return Err(Failure::new(2, "turn ids must be agent/generation/turn_id"));
    }
    if eliot_core::parse_agent_id(agent).is_err() {
        return Err(Failure::new(
            2,
            "turn ids must be agent/generation/turn_id with an a<number> agent; command ids containing `/` must use --command",
        ));
    }
    let generation: i64 = generation.parse().map_err(|_| {
        Failure::new(
            2,
            "turn ids must be agent/generation/turn_id with a nonnegative generation",
        )
    })?;
    if generation < 0 {
        return Err(Failure::new(
            2,
            "turn ids must be agent/generation/turn_id with a nonnegative generation",
        ));
    }
    let target = WaitTurnRef::new(agent, generation, turn_id);
    target
        .validate()
        .map_err(|_| Failure::new(2, "turn ids must be agent/generation/turn_id"))?;
    Ok(target)
}

/// Exit code for a settled `swarm wait`. Timeout (or an uncompleted snapshot)
/// exits 5; otherwise 0 only when every entry succeeded (`all`) or at least
/// one succeeded (`any`). Interrupted entries exit 7, unknown/not_found/
/// not_visible entries exit 4, and plain failures exit 1.
pub(crate) fn swarm_wait_exit_code(response: &SwarmWaitResponse) -> u8 {
    if response.timed_out || !response.completed {
        return 5;
    }
    let succeeded = response
        .results
        .iter()
        .filter(|entry| entry.status == "succeeded")
        .count();
    if response.mode == "any" && succeeded > 0 {
        return 0;
    }
    if response.mode == "all" && succeeded == response.results.len() && !response.results.is_empty()
    {
        return 0;
    }
    if response
        .results
        .iter()
        .any(|entry| entry.status == "interrupted")
    {
        return INTERRUPTED_EXIT_CODE;
    }
    if response.results.iter().any(|entry| {
        matches!(
            entry.status.as_str(),
            "unknown" | "not_found" | "not_visible"
        )
    }) {
        return 4;
    }
    1
}

pub(crate) async fn run_tree_change(
    agent: String,
    archived: Option<bool>,
    data_dir: PathBuf,
    json_output: bool,
    id: Option<String>,
) -> Result<u8, Failure> {
    let id = mutation_id(id)?;
    let method = if archived.is_some() {
        "agent.archive"
    } else {
        "agent.delete"
    };
    let mut params = json!({"id":id,"agent":agent});
    if let Some(archived) = archived {
        params["archived"] = json!(archived);
    }
    let value: Value = eliot_daemon::client_call_typed(&data_dir, method, &params)
        .await
        .map_err(Failure::for_mutation)?;
    print_value(&value, json_output);
    Ok(0)
}

#[cfg(test)]
mod tests {
    use super::{
        ExactTurnCapture, INTERRUPTED_EXIT_CODE, WaitTarget, explicit_wait_targets,
        missing_turn_failure, parse_wait_turn_ref, positional_wait_target, race_or_failure,
        swarm_wait_exit_code,
    };
    use crate::cli::args::{Cli, CliCommand, WaitArgs};
    use crate::cli::support::Failure;
    use clap::Parser;
    use eliot_core::Error as CoreError;
    use eliot_core::api::SwarmWaitResponse;
    #[test]
    fn swarm_wait_parse_and_exit_codes() {
        use eliot_core::api::WaitEntryResult;

        let turn = parse_wait_turn_ref("a1/2/turn-9").expect("turn parses");
        assert_eq!(turn.agent, "a1");
        assert_eq!(turn.generation, 2);
        assert_eq!(turn.turn_id, "turn-9");
        // Turn ids may contain slashes after the generation.
        let slashed = parse_wait_turn_ref("a1/2/turn/with/slash").expect("slashed turn parses");
        assert_eq!(slashed.turn_id, "turn/with/slash");
        for bad in ["a1", "a1/", "/1/t", "a1/x/t", "a1/-1/t", "a1/1/", " /1/t"] {
            assert!(parse_wait_turn_ref(bad).is_err(), "id: {bad}");
        }
        let completed = |mode: &str, statuses: &[&str]| SwarmWaitResponse {
            mode: mode.to_owned(),
            completed: true,
            timed_out: false,
            mail_wake: false,
            results: statuses
                .iter()
                .enumerate()
                .map(|(index, status)| {
                    WaitEntryResult::new("command", format!("c{index}"), (*status).to_owned())
                })
                .collect(),
        };
        assert_eq!(
            swarm_wait_exit_code(&completed("all", &["succeeded", "succeeded"])),
            0
        );
        assert_eq!(
            swarm_wait_exit_code(&completed("any", &["failed", "succeeded"])),
            0
        );
        assert_eq!(
            swarm_wait_exit_code(&completed("all", &["succeeded", "failed"])),
            1
        );
        assert_eq!(swarm_wait_exit_code(&completed("any", &["failed"])), 1);
        assert_eq!(
            swarm_wait_exit_code(&completed("all", &["succeeded", "unknown"])),
            4
        );
        assert_eq!(swarm_wait_exit_code(&completed("any", &["not_found"])), 4);
        assert_eq!(
            swarm_wait_exit_code(&completed("all", &["succeeded", "interrupted"])),
            INTERRUPTED_EXIT_CODE
        );
        let timed_out = SwarmWaitResponse {
            mode: "all".to_owned(),
            completed: false,
            timed_out: true,
            mail_wake: false,
            results: vec![WaitEntryResult::new("command", "c0", "still_running")],
        };
        assert_eq!(swarm_wait_exit_code(&timed_out), 5);
        let cli = Cli::try_parse_from(["swarm", "wait", "--all", "--timeout-seconds", "7", "c1"])
            .unwrap();
        let Some(CliCommand::Wait(WaitArgs {
            all,
            any,
            timeout_seconds,
            ids,
            command,
            turn,
            mail,
        })) = cli.command
        else {
            panic!("wait command required");
        };
        assert!(all);
        assert!(!any);
        assert_eq!(timeout_seconds, 7);
        assert_eq!(ids, vec!["c1".to_owned()]);
        assert!(command.is_empty());
        assert!(turn.is_empty());
        assert!(!mail);
        let cli = Cli::try_parse_from(["swarm", "wait", "--all", "--mail", "c1"]).unwrap();
        let Some(CliCommand::Wait(WaitArgs { mail, .. })) = cli.command else {
            panic!("wait command required");
        };
        assert!(mail);
        let cli = Cli::try_parse_from([
            "swarm",
            "wait",
            "--all",
            "--command",
            "01ARZ3NDEKTSV4RRFFQ69G5FAV/send",
            "--command",
            "c2",
            "--turn",
            "a1/2/t",
        ])
        .unwrap();
        let Some(CliCommand::Wait(WaitArgs { command, turn, .. })) = cli.command else {
            panic!("wait command required");
        };
        assert_eq!(
            command,
            vec![
                "01ARZ3NDEKTSV4RRFFQ69G5FAV/send".to_owned(),
                "c2".to_owned()
            ]
        );
        assert_eq!(turn, vec!["a1/2/t".to_owned()]);
    }

    #[test]
    fn positional_ids_keep_their_historical_meaning() {
        // A `cmd:`-prefixed id is an opaque command preserved verbatim: no
        // prefix is stripped and no turn is inferred.
        match positional_wait_target("swarm wait", "cmd:legacy").expect("parses") {
            WaitTarget::Command(id) => assert_eq!(id, "cmd:legacy"),
            WaitTarget::Turn(_) => panic!("cmd:legacy must stay an opaque command"),
        }
        // A slashed positional is a turn ref only when the first segment
        // is an Agent id; a bare `<ULID>/2/t` is rejected (use --command or
        // --turn for disambiguation).
        assert!(
            positional_wait_target("swarm wait", "01ARZ3NDEKTSV4RRFFQ69G5FAV/2/t").is_err(),
            "a ULID first segment cannot be an Agent id"
        );
        match positional_wait_target("swarm wait", "a1/2/t").expect("parses") {
            WaitTarget::Turn(turn) => {
                assert_eq!(
                    (turn.agent.as_str(), turn.generation, turn.turn_id.as_str()),
                    ("a1", 2, "t")
                );
            }
            WaitTarget::Command(_) => panic!("valid turn ref must stay a turn"),
        }
        // Linked and nested command ids are still rejected positionally;
        // they need `--command`.
        for id in [
            "01ARZ3NDEKTSV4RRFFQ69G5FAV/send",
            "01ARZ3NDEKTSV4RRFFQ69G5FAV/launch/send",
        ] {
            assert!(
                positional_wait_target("swarm wait", id).is_err(),
                "positional {id} keeps its historical rejection"
            );
        }
        // Plain command ids and bare triples are unchanged.
        match positional_wait_target("swarm wait", "01ARZ3NDEKTSV4RRFFQ69G5FAV").expect("parses") {
            WaitTarget::Command(id) => assert_eq!(id, "01ARZ3NDEKTSV4RRFFQ69G5FAV"),
            WaitTarget::Turn(_) => panic!("plain id must stay a command"),
        }
        // `splitn(3, .../...)` keeps the historical meaning: the turn id keeps
        // the remainder verbatim.
        match positional_wait_target("swarm wait", "a1/2/t/with/slash").expect("parses") {
            WaitTarget::Turn(turn) => assert_eq!(turn.turn_id, "t/with/slash"),
            WaitTarget::Command(_) => panic!("bare triple must stay a turn"),
        }
        for bad in ["", "   "] {
            assert!(
                positional_wait_target("swarm wait", bad).is_err(),
                "id: {bad}"
            );
            assert!(
                positional_wait_target("swarm digest", bad).is_err(),
                "id: {bad}"
            );
        }
    }

    #[test]
    fn explicit_options_disambiguate_slash_ids() {
        // `--command` preserves linked and nested ids verbatim; `--turn`
        // keeps the strict triple shape.
        let mut commands = vec!["01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned()];
        let mut turns = Vec::new();
        explicit_wait_targets(
            "swarm wait",
            &mut commands,
            &mut turns,
            &[
                "01ARZ3NDEKTSV4RRFFQ69G5FAV/send".to_owned(),
                "01ARZ3NDEKTSV4RRFFQ69G5FAV/launch/send".to_owned(),
                "cmd:legacy".to_owned(),
                "a1/2/t".to_owned(),
            ],
            &["a1/2/t".to_owned()],
        )
        .expect("explicit targets parse");
        assert_eq!(
            commands,
            vec![
                "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                "01ARZ3NDEKTSV4RRFFQ69G5FAV/send".to_owned(),
                "01ARZ3NDEKTSV4RRFFQ69G5FAV/launch/send".to_owned(),
                "cmd:legacy".to_owned(),
                "a1/2/t".to_owned(),
            ]
        );
        assert_eq!(turns.len(), 1);
        assert_eq!(
            (
                turns[0].agent.as_str(),
                turns[0].generation,
                turns[0].turn_id.as_str()
            ),
            ("a1", 2, "t")
        );
        // Blank commands, malformed turns, and non-agent `--turn` first
        // segments are usage errors.
        assert!(
            explicit_wait_targets(
                "swarm wait",
                &mut Vec::new(),
                &mut Vec::new(),
                &[],
                &["01ARZ3NDEKTSV4RRFFQ69G5FAV/2/t".to_owned()],
            )
            .is_err(),
            "--turn still requires an a<number> agent"
        );
        // A triple-shaped command id rides `--command` verbatim.
        let mut commands = vec![];
        let mut turns = Vec::new();
        explicit_wait_targets(
            "swarm wait",
            &mut commands,
            &mut turns,
            &["01ARZ3NDEKTSV4RRFFQ69G5FAV/2/t".to_owned()],
            &[],
        )
        .expect("triple-shaped command parses");
        assert_eq!(commands, vec!["01ARZ3NDEKTSV4RRFFQ69G5FAV/2/t".to_owned()]);
        // Blank commands and malformed turns are usage errors.
        let mut commands = Vec::new();
        let mut turns = Vec::new();
        assert!(
            explicit_wait_targets(
                "swarm wait",
                &mut commands,
                &mut turns,
                &["  ".to_owned()],
                &[]
            )
            .is_err()
        );
        assert!(
            explicit_wait_targets(
                "swarm digest",
                &mut commands,
                &mut turns,
                &[],
                &["a1/x/t".to_owned()]
            )
            .is_err()
        );
    }

    #[test]
    fn exact_turn_reports_missing_turn_and_race() {
        let missing = missing_turn_failure("steer", "a1");
        assert_eq!(missing.exit_code, 3);
        assert!(missing.message.contains("a1"), "{missing:?}");
        assert!(missing.message.contains("no active turn"), "{missing:?}");

        let capture = ExactTurnCapture {
            verb: "interrupt",
            agent: "a2".to_owned(),
            generation: 4,
            turn_id: "turn-9".to_owned(),
        };
        let race = capture.race();
        assert_eq!(race.exit_code, 3);
        assert!(race.message.contains("race"), "{race:?}");
        assert!(race.message.contains("a2"), "{race:?}");
        assert!(race.message.contains("generation=4"), "{race:?}");
        assert!(race.message.contains("turn-9"), "{race:?}");
        assert!(race.message.contains("not retried"), "{race:?}");
    }

    #[test]
    fn stale_rejection_of_a_capture_is_a_race() {
        let capture = ExactTurnCapture {
            verb: "steer",
            agent: "a1".to_owned(),
            generation: 2,
            turn_id: "turn-3".to_owned(),
        };
        let stale = Failure::from_core(CoreError::new("stale_generation", "turn moved"), true);
        assert_eq!(stale.exit_code, 3);
        let race = race_or_failure(Some(&capture), stale);
        assert_eq!(race.exit_code, 3);
        assert!(race.message.contains("race"), "{race:?}");
        assert!(race.message.contains("turn-3"), "{race:?}");

        // Explicit submits keep the untouched rejection: no capture, no race.
        let stale = Failure::from_core(CoreError::new("stale_generation", "turn moved"), true);
        let untouched = race_or_failure(None, stale);
        assert_eq!(untouched.exit_code, 3);
        assert!(!untouched.message.contains("race"), "{untouched:?}");

        // Non-stale failures pass through even after a capture.
        for code in ["bad_state", "not_found", "failed", "internal"] {
            let failure = Failure::from_core(CoreError::new(code, "other"), true);
            let expected_exit = failure.exit_code;
            let passed = race_or_failure(Some(&capture), failure);
            assert_eq!(passed.exit_code, expected_exit, "code={code}");
            assert!(!passed.message.contains("race"), "code={code}");
        }
    }
}
