use super::lifecycle::cancel_unstarted_resume_input_tx;
use super::*;

const WORKSPACE_COLUMNS: &str = "agent, target_generation, start_command_id, project, repo_path,
    repo_common_dir, requested_branch, base_ref, base_oid, worktree_path, frozen_setup,
    phase, setup_index, outcome, updated_at";

pub(super) fn workspace_from_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<WorkspacePreparation> {
    let frozen_setup: Vec<core::WorkspaceSetup> = serde_json::from_str(&row.get::<_, String>(10)?)
        .map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                10,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?;
    let phase = parse_enum(&row.get::<_, String>(11)?, "workspace phase").map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(11, rusqlite::types::Type::Text, Box::new(error))
    })?;
    let outcome = row
        .get::<_, Option<String>>(13)?
        .map(|value| serde_json::from_str(&value))
        .transpose()
        .map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                13,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?;
    let setup_index = usize::try_from(row.get::<_, i64>(12)?).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            12,
            rusqlite::types::Type::Integer,
            Box::new(error),
        )
    })?;
    if setup_index > frozen_setup.len() {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            12,
            rusqlite::types::Type::Integer,
            Box::new(Error::new(
                "invalid",
                "stored setup index exceeds the frozen setup length",
            )),
        ));
    }
    Ok(WorkspacePreparation {
        agent: format_agent_id(row.get(0)?),
        target_generation: row.get(1)?,
        start_command_id: row.get(2)?,
        project: row.get(3)?,
        repo_path: row.get(4)?,
        repo_common_dir: row.get(5)?,
        requested_branch: row.get(6)?,
        base_ref: row.get(7)?,
        base_oid: row.get(8)?,
        worktree_path: row.get(9)?,
        frozen_setup,
        phase,
        setup_index,
        outcome,
        updated_at: row.get(14)?,
    })
}

pub(super) fn load_workspace(
    connection: &Connection,
    agent: i64,
) -> Result<Option<WorkspacePreparation>> {
    connection
        .query_row(
            &format!("SELECT {WORKSPACE_COLUMNS} FROM workspaces WHERE agent = ?1"),
            [agent],
            workspace_from_row,
        )
        .optional()
        .map_err(sql_error)
}

pub(super) fn load_workspace_by_path(
    connection: &Connection,
    path: &str,
) -> Result<Option<WorkspacePreparation>> {
    connection
        .query_row(
            &format!("SELECT {WORKSPACE_COLUMNS} FROM workspaces WHERE worktree_path = ?1"),
            [path],
            workspace_from_row,
        )
        .optional()
        .map_err(sql_error)
}

pub(super) fn workspace_phase_name(phase: WorkspacePhase) -> &'static str {
    match phase {
        WorkspacePhase::Creating => "creating",
        WorkspacePhase::SetupReady => "setup_ready",
        WorkspacePhase::SetupRunning => "setup_running",
        WorkspacePhase::Ready => "ready",
        WorkspacePhase::Unknown => "unknown",
        WorkspacePhase::Failed => "failed",
        WorkspacePhase::Removing => "removing",
        WorkspacePhase::Removed => "removed",
    }
}

pub(super) fn workspace_cancel_requested(workspace: &WorkspacePreparation) -> bool {
    workspace
        .outcome
        .as_ref()
        .and_then(|outcome| outcome.get("cancel_requested"))
        .and_then(Value::as_bool)
        == Some(true)
}

pub(super) fn require_absolute_workspace_path(path: &str) -> Result<()> {
    let bytes = path.as_bytes();
    let absolute = path.starts_with('/')
        || path.starts_with("\\\\")
        || (bytes.len() >= 3
            && bytes[0].is_ascii_alphabetic()
            && bytes[1] == b':'
            && matches!(bytes[2], b'/' | b'\\'));
    if !absolute
        || path.contains('\0')
        || path.starts_with("\\\\.\\")
        || path.replace('\\', "/").split('/').any(|part| part == "..")
    {
        return Err(Error::new(
            "invalid",
            "workspace reference must be an absolute non-traversing path",
        ));
    }
    Ok(())
}

pub(super) fn validate_setup_file(file: &FrozenSetupFile) -> Result<()> {
    require_absolute_workspace_path(&file.canonical_path)?;
    if file.size > i64::MAX as u64
        || file.sha256.len() != 64
        || !file.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(Error::new(
            "invalid",
            "invalid frozen setup file size or SHA-256",
        ));
    }
    Ok(())
}

pub(super) fn validate_workspace_input(input: &WorkspacePreparationInput) -> Result<()> {
    if encode_json(input)?.len() > MAX_PAGE_BYTES {
        return Err(Error::new(
            "frame_too_large",
            "workspace preparation exceeds its bound",
        ));
    }
    require_nonempty(&input.project, "workspace project")?;
    require_nonempty(&input.requested_branch, "workspace branch")?;
    if input.requested_branch.starts_with('-') || input.requested_branch.contains('\0') {
        return Err(Error::new("invalid", "invalid requested workspace branch"));
    }
    if let Some(base) = &input.base_ref {
        require_nonempty(base, "workspace base ref")?;
    }
    validate_git_oid(&input.base_oid)?;
    for path in [
        &input.repo_path,
        &input.repo_common_dir,
        &input.worktree_path,
    ] {
        require_absolute_workspace_path(path)?;
    }
    if input.repo_path == input.worktree_path {
        return Err(Error::new(
            "invalid",
            "prepared worktree must differ from owner repository",
        ));
    }
    for setup in &input.frozen_setup {
        validate_setup_file(&setup.program)?;
        if setup.timeout_ms == 0 || setup.timeout_ms > i64::MAX as u64 {
            return Err(Error::new("invalid", "invalid setup timeout"));
        }
        for arg in &setup.args {
            match arg {
                WorkspaceSetupArg::Literal(value) if value.contains('\0') => {
                    return Err(Error::new("invalid", "setup argument contains NUL"));
                }
                WorkspaceSetupArg::Literal(_) => {}
                WorkspaceSetupArg::File(file) => validate_setup_file(file)?,
            }
        }
    }
    Ok(())
}

pub(super) fn managed_path_contains(root: &str, candidate: &str) -> bool {
    fn normalized(path: &str) -> String {
        #[cfg(windows)]
        {
            let path = path.replace('\\', "/");
            let path = match path.strip_prefix("//?/UNC/") {
                Some(rest) => format!("//{rest}"),
                None => path.strip_prefix("//?/").unwrap_or(&path).to_owned(),
            };
            path.to_ascii_lowercase()
        }
        #[cfg(not(windows))]
        {
            path.to_owned()
        }
    }
    let root = normalized(root);
    let candidate = normalized(candidate);
    let mut candidate = Path::new(&candidate).components();
    Path::new(&root)
        .components()
        .all(|component| candidate.next() == Some(component))
}

pub(super) fn require_managed_path_ready(connection: &Connection, path: &str) -> Result<()> {
    // Check every containing root, including a non-ready parent of a nested checkout.
    // Stream only path/status candidates; never load frozen setup payloads here.
    let mut statement = connection
        .prepare(
            "SELECT worktree_path FROM workspaces
         WHERE phase <> 'ready' OR json_extract(outcome, '$.cancel_requested') = 1",
        )
        .map_err(sql_error)?;
    let roots = statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(sql_error)?;
    for root in roots {
        if managed_path_contains(&root.map_err(sql_error)?, path) {
            return Err(Error::new(
                "bad_state",
                "containing managed workspace is not ready or was removed",
            ));
        }
    }
    Ok(())
}

pub(super) fn launch_snapshot_with_native_store(
    agent: &Agent,
    expected: &Agent,
    binding: &NativeStoreBinding,
    command_id: &str,
) -> Result<String> {
    if expected.id != agent.id
        || expected.generation != 0
        || expected.state != AgentState::Created
        || expected.harness != agent.harness
        || expected.profile != agent.profile
        || expected.workspace != agent.workspace
        || expected.profile_snapshot != agent.profile_snapshot
    {
        return Err(Error::new(
            "stale_generation",
            "launch target changed during native store verification",
        ));
    }
    let mut snapshot = agent.profile_snapshot.clone();
    let object = snapshot
        .as_object_mut()
        .ok_or_else(|| Error::new("invalid", "frozen profile snapshot must be an object"))?;
    if let Some(retained) = object.get("native_store_binding") {
        let retained: NativeStoreBinding =
            serde_json::from_value(retained.clone()).map_err(|_| {
                Error::new(
                    "source_integrity",
                    "retained native store binding is invalid",
                )
            })?;
        if retained != *binding {
            return Err(Error::new(
                "source_integrity",
                "retained native store binding cannot be replaced",
            ));
        }
    } else {
        object.insert(
            "native_store_binding".to_owned(),
            serde_json::to_value(binding)
                .map_err(|error| Error::new("invalid", error.to_string()))?,
        );
    }
    let encoded = encode_json(&snapshot)?;
    let mut start = core::AgentStartParams::new(&agent.id, 1, command_id);
    start.profile = Some(snapshot);
    start.workspace = agent.workspace.clone();
    // Runtime environment/readback are supplied and checked by Host at send.
    check_rpc_frame("agent.start", &start)?;
    Ok(encoded)
}

pub(super) fn gate_workspace_start(
    connection: &Connection,
    agent: &Agent,
    command: &Command,
    target: i64,
) -> Result<()> {
    if let Some(path) = &agent.workspace {
        require_managed_path_ready(connection, path)?;
    }
    if let Some(workspace) = load_workspace(connection, parse_agent_id(&agent.id)?)?
        && (workspace.phase != WorkspacePhase::Ready
            || workspace.target_generation != target
            || workspace.start_command_id != command.id
            || workspace_cancel_requested(&workspace)
            || agent.workspace.as_deref() != Some(workspace.worktree_path.as_str()))
    {
        return Err(Error::new(
            "bad_state",
            "native start requires its exact ready workspace preparation",
        ));
    }
    Ok(())
}

pub(super) fn require_workspace_removable(
    connection: &Connection,
    agent: &Agent,
    workspace: &WorkspacePreparation,
) -> Result<()> {
    if !agent.is_terminal()
        || agent.active_turn.is_some()
        || matches!(
            workspace.phase,
            WorkspacePhase::Creating
                | WorkspacePhase::SetupReady
                | WorkspacePhase::SetupRunning
                | WorkspacePhase::Removing
                | WorkspacePhase::Removed
        )
        || workspace
            .outcome
            .as_ref()
            .and_then(|value| value.get("stage"))
            .and_then(Value::as_str)
            == Some("removal")
            && workspace.phase == WorkspacePhase::Unknown
            && workspace
                .outcome
                .as_ref()
                .and_then(|value| value.get("result"))
                .and_then(|value| value.get("state"))
                .and_then(Value::as_str)
                != Some("failed")
    {
        return Err(Error::new(
            "bad_state",
            "worktree removal requires a terminal owner and a settled preparation",
        ));
    }
    let start = load_command_record(connection, &workspace.start_command_id)?
        .ok_or_else(|| Error::new("source_integrity", "workspace start command is missing"))?;
    if matches!(start.state, CommandState::Accepted | CommandState::Queued) {
        return Err(Error::new(
            "busy",
            "cancel the pending start before removing its workspace",
        ));
    }
    let mut statement = connection
        .prepare(
            "SELECT workspace FROM agents WHERE id <> ?1 AND workspace IS NOT NULL
         AND state IN ('created','starting','idle','busy','waiting','unknown')",
        )
        .map_err(sql_error)?;
    let borrowers = statement
        .query_map([parse_agent_id(&agent.id)?], |row| row.get::<_, String>(0))
        .map_err(sql_error)?;
    for borrower in borrowers {
        if managed_path_contains(&workspace.worktree_path, &borrower.map_err(sql_error)?) {
            return Err(Error::new(
                "busy",
                "another nonterminal Agent references this worktree or a descendant",
            ));
        }
    }
    let mut sources = connection
        .prepare("SELECT repo_path FROM workspaces WHERE agent <> ?1 AND phase <> 'removed'")
        .map_err(sql_error)?;
    let sources = sources
        .query_map([parse_agent_id(&agent.id)?], |row| row.get::<_, String>(0))
        .map_err(sql_error)?;
    for source in sources {
        if managed_path_contains(&workspace.worktree_path, &source.map_err(sql_error)?) {
            return Err(Error::new(
                "busy",
                "another retained worktree depends on this source repository",
            ));
        }
    }
    let mut inputs = connection
        .prepare(
            "SELECT w.frozen_setup, w.setup_index FROM workspaces w
         JOIN commands c ON c.id = w.start_command_id
         WHERE w.phase <> 'removed' AND w.setup_index < json_array_length(w.frozen_setup)
           AND c.state IN ('accepted','queued','dispatched','unknown')",
        )
        .map_err(sql_error)?;
    let inputs = inputs
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })
        .map_err(sql_error)?;
    for input in inputs {
        let (setup, index) = input.map_err(sql_error)?;
        let setup: Vec<core::WorkspaceSetup> = serde_json::from_str(&setup).map_err(|error| {
            Error::new("internal", format!("invalid retained setup input: {error}"))
        })?;
        let index = usize::try_from(index)
            .map_err(|_| Error::new("internal", "invalid retained setup index"))?;
        if index > setup.len() {
            return Err(Error::new(
                "internal",
                "retained setup index exceeds input length",
            ));
        }
        for entry in setup.iter().skip(index) {
            let referenced =
                managed_path_contains(&workspace.worktree_path, &entry.program.canonical_path)
                    || entry.args.iter().any(|arg| match arg {
                        WorkspaceSetupArg::File(file) => {
                            managed_path_contains(&workspace.worktree_path, &file.canonical_path)
                        }
                        WorkspaceSetupArg::Literal(_) => false,
                    });
            if referenced {
                return Err(Error::new(
                    "busy",
                    "pending setup retains a declared input inside this worktree",
                ));
            }
        }
    }
    Ok(())
}

pub(super) fn require_workspace_sources_ready(
    connection: &Connection,
    repo_path: &str,
    setup: &[core::WorkspaceSetup],
    index: usize,
) -> Result<()> {
    require_managed_path_ready(connection, repo_path)?;
    for entry in setup.iter().skip(index) {
        require_managed_path_ready(connection, &entry.program.canonical_path)?;
        for arg in &entry.args {
            if let WorkspaceSetupArg::File(file) = arg {
                require_managed_path_ready(connection, &file.canonical_path)?;
            }
        }
    }
    Ok(())
}

pub(super) fn persist_workspace(
    tx: &Transaction<'_>,
    old: &WorkspacePreparation,
    next: &WorkspacePreparation,
) -> Result<()> {
    if encode_json(next)?.len() > MAX_PAGE_BYTES {
        return Err(Error::new(
            "frame_too_large",
            "workspace row exceeds readback bound",
        ));
    }
    let changed = tx.execute(
        "UPDATE workspaces SET target_generation = ?1, start_command_id = ?2, phase = ?3, setup_index = ?4, outcome = ?5, updated_at = ?6
         WHERE agent = ?7 AND start_command_id = ?8 AND target_generation = ?9 AND phase = ?10 AND setup_index = ?11 AND outcome IS ?12",
        params![next.target_generation, next.start_command_id, workspace_phase_name(next.phase), next.setup_index as i64,
            next.outcome.as_ref().map(encode_json).transpose()?, next.updated_at, parse_agent_id(&old.agent)?,
            old.start_command_id, old.target_generation, workspace_phase_name(old.phase), old.setup_index as i64,
            old.outcome.as_ref().map(encode_json).transpose()?],
    ).map_err(sql_error)?;
    if changed != 1 {
        return Err(Error::new(
            "stale_generation",
            "workspace preparation marker changed",
        ));
    }
    Ok(())
}

pub(super) fn workspace_event(
    tx: &Transaction<'_>,
    workspace: &WorkspacePreparation,
    now: i64,
) -> Result<Event> {
    insert_daemon_event(
        tx,
        now,
        &format!("workspace.{}", workspace_phase_name(workspace.phase)),
        "workspace",
        Some(parse_agent_id(&workspace.agent)?),
        Some(workspace.target_generation),
        json!({"start_command_id": workspace.start_command_id, "phase": workspace.phase,
            "setup_index": workspace.setup_index, "target_generation": workspace.target_generation, "outcome": workspace.outcome}),
    )
}

pub(super) fn preserve_native_git_write_marker(previous: Option<&Value>, outcome: &mut Value) {
    if let Some(marker) = previous.and_then(|value| value.get("native_git_write_dispatched")) {
        outcome["native_git_write_dispatched"] = marker.clone();
    }
}

pub(super) fn admit_workspace_preparation(
    tx: &Transaction<'_>,
    request: &SubmitCommand,
    input: Option<&WorkspacePreparationInput>,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    let command = load_command_record(tx, &request.id)?
        .ok_or_else(|| Error::new("internal", "start command disappeared"))?;
    let agent_num = command
        .target
        .ok_or_else(|| Error::new("internal", "start has no Agent"))?;
    let agent = load_agent_by_num(tx, agent_num)?
        .ok_or_else(|| Error::new("internal", "start Agent disappeared"))?;
    let target = agent
        .generation
        .checked_add(1)
        .ok_or_else(|| Error::new("bad_state", "generation overflow"))?;
    if request.verb == "launch" {
        let Some(input) = input else {
            return Ok(());
        };
        require_workspace_sources_ready(tx, &input.repo_path, &input.frozen_setup, 0)?;
        let workspace = WorkspacePreparation {
            agent: agent.id,
            target_generation: target,
            start_command_id: request.id.clone(),
            project: input.project.clone(),
            repo_path: input.repo_path.clone(),
            repo_common_dir: input.repo_common_dir.clone(),
            requested_branch: input.requested_branch.clone(),
            base_ref: input.base_ref.clone(),
            base_oid: input.base_oid.clone(),
            worktree_path: input.worktree_path.clone(),
            frozen_setup: input.frozen_setup.clone(),
            phase: WorkspacePhase::Creating,
            setup_index: 0,
            outcome: Some(json!({
                "stage": "creation",
                "reuse": false,
                "native_git_write_dispatched": false,
            })),
            updated_at: now,
        };
        if encode_json(&workspace)?.len() > MAX_PAGE_BYTES {
            return Err(Error::new(
                "frame_too_large",
                "workspace row exceeds readback bound",
            ));
        }
        db!(tx.execute(
            "INSERT INTO workspaces(agent,target_generation,start_command_id,project,repo_path,repo_common_dir,requested_branch,base_ref,base_oid,
             worktree_path,frozen_setup,phase,setup_index,outcome,updated_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,'creating',0,?12,?13)",
            params![agent_num, target, workspace.start_command_id, workspace.project, workspace.repo_path, workspace.repo_common_dir,
                workspace.requested_branch, workspace.base_ref, workspace.base_oid, workspace.worktree_path, encode_json(&workspace.frozen_setup)?,
                workspace.outcome.as_ref().map(encode_json).transpose()?, now],
        ));
        events.push(workspace_event(tx, &workspace, now)?);
    } else if let Some(mut workspace) = load_workspace(tx, agent_num)? {
        if workspace.phase != WorkspacePhase::Ready
            || workspace_cancel_requested(&workspace)
            || workspace.target_generation != agent.generation
            || workspace.setup_index != workspace.frozen_setup.len()
        {
            return Err(Error::new(
                "bad_state",
                "resume requires a previously ready retained worktree",
            ));
        }
        if let Some(input) = input {
            let retained: WorkspacePreparationInput = serde_json::from_value(json!(&workspace))
                .map_err(|_| Error::new("internal", "invalid retained workspace"))?;
            if *input != retained {
                return Err(Error::new(
                    "id_conflict",
                    "resume cannot replace frozen workspace inputs",
                ));
            }
        }
        require_workspace_sources_ready(
            tx,
            &workspace.repo_path,
            &workspace.frozen_setup,
            workspace.setup_index,
        )?;
        let old = workspace.clone();
        let mut previous_outcome = old.outcome.clone().unwrap_or(Value::Null);
        if let Some(outcome) = previous_outcome.as_object_mut() {
            outcome.remove("resume_previous");
        }
        let resume_previous = json!({
            "start_command_id": old.start_command_id.as_str(),
            "target_generation": old.target_generation,
            "outcome": previous_outcome,
        });
        workspace.start_command_id = request.id.clone();
        workspace.target_generation = target;
        workspace.phase = WorkspacePhase::Creating;
        workspace.outcome = Some(json!({
            "stage": "creation",
            "reuse": true,
            "native_git_write_dispatched": false,
            "resume_previous": resume_previous,
        }));
        workspace.updated_at = now;
        persist_workspace(tx, &old, &workspace)?;
        events.push(workspace_event(tx, &workspace, now)?);
        close_finished_workspace_process_source(tx, agent_num, old.target_generation, now, events)?;
    } else if input.is_some() {
        return Err(Error::new(
            "unsupported",
            "resume does not create a replacement for an unmanaged workspace",
        ));
    }
    Ok(())
}

pub(super) fn preparation_command(
    connection: &Connection,
    workspace: &WorkspacePreparation,
) -> Result<(CommandRecord, Agent)> {
    let command = load_command_record(connection, &workspace.start_command_id)?
        .ok_or_else(|| Error::new("not_found", "workspace start command was not found"))?;
    let agent = load_agent_by_num(connection, parse_agent_id(&workspace.agent)?)?
        .ok_or_else(|| Error::new("not_found", "workspace Agent was not found"))?;
    if command.target != Some(parse_agent_id(&workspace.agent)?)
        || !matches!(command.command.verb.as_str(), "launch" | "resume")
        || agent.generation.checked_add(1) != Some(workspace.target_generation)
        || command.command.generation != Some(agent.generation)
        || agent.active_turn.is_some()
        || command
            .command
            .expected_generation
            .is_some_and(|expected| expected != agent.generation)
        || (command.command.verb == "launch"
            && (agent.generation != 0 || agent.state != AgentState::Created))
        || (command.command.verb == "resume" && !agent.is_terminal())
    {
        return Err(Error::new(
            "stale_generation",
            "preparation no longer precedes its exact native dispatch",
        ));
    }
    Ok((command, agent))
}

pub(super) fn cancel_workspace_input(
    tx: &Transaction<'_>,
    command: &str,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    if let Some(linked) = load_linked_command(tx, command)?
        && matches!(linked.state, CommandState::Accepted | CommandState::Queued)
    {
        let outcome = json!({"stage": "workspace", "reason": "start_not_dispatched"});
        update_command_state(
            tx,
            &linked.id,
            CommandState::Cancelled,
            Some(&outcome),
            None,
            now,
        )?;
        events.push(insert_daemon_command_event(
            tx,
            now,
            "command.cancelled",
            linked.target.as_deref().map(parse_agent_id).transpose()?,
            linked.generation,
            &linked.id,
            json!({"command_id": linked.id, "reason": "workspace_preparation"}),
        )?);
    }
    Ok(())
}

pub(super) fn settle_preparation_command(
    tx: &Transaction<'_>,
    workspace: &WorkspacePreparation,
    state: CommandState,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<Option<WorkspacePreparation>> {
    let (command, agent) = preparation_command(tx, workspace)?;
    if !matches!(
        command.state,
        CommandState::Accepted | CommandState::Queued | CommandState::Unknown
    ) || !core::workspace_preparation_transition_allowed(command.state, state)
    {
        return Err(Error::new(
            "bad_state",
            "workspace start is already settled or dispatched",
        ));
    }
    let outcome = json!({"stage": "workspace", "native_dispatched": false, "target_generation": workspace.target_generation, "detail": workspace.outcome});
    // preparation_command plus the typed workspace caller prove the pre-native
    // marker/verb/generation exception; ordinary settlement cannot use it.
    write_command_state(tx, &command.command.id, state, Some(&outcome), None, now)?;
    let restored_workspace = if command.command.verb == "resume" && state == CommandState::Cancelled
    {
        // WorkspaceAction::Cancel reaches this branch only after validating the
        // exact dispatched Stop and its confirmed Stopped result.
        Some(
            cancel_unstarted_resume_input_tx(
                tx,
                parse_agent_id(&agent.id)?,
                &command.command.id,
                "workspace_stop_confirmed",
                now,
                events,
                true,
            )?
            .ok_or_else(|| {
                Error::new(
                    "source_integrity",
                    "confirmed resume cancellation lost its retained workspace",
                )
            })?,
        )
    } else {
        if matches!(state, CommandState::Failed | CommandState::Cancelled) {
            capacity::release_unstarted_tx(
                tx,
                parse_agent_id(&agent.id)?,
                workspace.target_generation,
                &command.command.id,
            )?;
        }
        if command.command.verb == "launch" {
            let next = match state {
                CommandState::Failed => AgentState::Failed,
                CommandState::Cancelled => AgentState::Stopped,
                _ => AgentState::Created,
            };
            db!(tx.execute("UPDATE agents SET state = ?1, updated_at = ?2 WHERE id = ?3 AND generation = 0 AND state = 'created'",
                params![agent_state_name(next), now, parse_agent_id(&agent.id)?]));
        }
        if matches!(state, CommandState::Failed | CommandState::Cancelled) {
            cancel_workspace_input(tx, &command.command.id, now, events)?;
        }
        None
    };
    events.push(insert_daemon_command_event(
        tx,
        now,
        &format!("command.{}", command_state_name(state)),
        Some(parse_agent_id(&agent.id)?),
        Some(agent.generation),
        &command.command.id,
        json!({"command_id": command.command.id, "outcome": outcome}),
    )?);
    Ok(restored_workspace)
}

enum WorkspaceAction {
    Creation(WorkspaceCreationOutcome),
    Begin(usize),
    Setup(usize, WorkspaceSetupOutcome),
    CancelRequest(String),
    Cancel(String, WorkspaceCancelOutcome),
    Removal(WorkspaceRemovalOutcome),
}

impl Store {
    pub fn get_workspace(&self, agent: &str) -> Result<Option<WorkspacePreparation>> {
        load_workspace(&self.connection, parse_agent_id(agent)?)
    }

    pub fn get_workspace_by_path(&self, path: &str) -> Result<Option<WorkspacePreparation>> {
        require_nonempty(path, "managed workspace path")?;
        load_workspace_by_path(&self.connection, path)
    }

    /// Physical cleanup must wait for every containing workspace's Git source to drain.
    pub fn has_open_git_sources_within(&self, worktree_path: &str) -> Result<bool> {
        require_absolute_workspace_path(worktree_path)?;
        let mut statement = self
            .connection
            .prepare(
                "SELECT COALESCE(
                 (SELECT json_extract(e.data, '$.workspace') FROM events e
                  WHERE e.agent = s.agent AND e.generation = s.generation
                    AND e.kind = 'source.opened' AND json_extract(e.data, '$.source') = 'git'
                  ORDER BY e.seq DESC LIMIT 1), a.workspace)
             FROM source_state s LEFT JOIN agents a ON a.id = s.agent
             WHERE s.source = 'git' AND s.state = 'open'",
            )
            .map_err(sql_error)?;
        let paths = statement
            .query_map([], |row| row.get::<_, Option<String>>(0))
            .map_err(sql_error)?;
        for path in paths {
            let path = path
                .map_err(sql_error)?
                .filter(|path| !path.trim().is_empty())
                .ok_or_else(|| {
                    Error::new(
                        "source_integrity",
                        "open Git source has no retained workspace identity",
                    )
                })?;
            if managed_path_contains(worktree_path, &path) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Runtime recovery/cleanup candidates only; settled workspace history stays
    /// durable without allocating an operation slot in the daemon.
    pub fn list_pending_workspace_preparations(
        &self,
        after_agent: Option<&str>,
        limit: usize,
    ) -> Result<Vec<WorkspacePreparation>> {
        validate_limit(limit)?;
        let after = after_agent.map(parse_agent_id).transpose()?.unwrap_or(0);
        let mut statement = self
            .connection
            .prepare(&format!(
                "SELECT {WORKSPACE_COLUMNS} FROM workspaces WHERE agent > ?1
             AND phase <> 'removed' AND (
                 phase IN ('unknown', 'removing') OR EXISTS (
                     SELECT 1 FROM commands c WHERE c.id = workspaces.start_command_id
                     AND c.state IN ('accepted', 'queued', 'unknown')
                 )
             ) ORDER BY agent LIMIT ?2"
            ))
            .map_err(sql_error)?;
        let rows = statement
            .query_map(params![after, limit as i64], workspace_from_row)
            .map_err(sql_error)?;
        let mut workspaces = rows
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(sql_error)?;
        enforce_page_bytes(&mut workspaces)?;
        Ok(workspaces)
    }

    /// Reserve one native managed-worktree Git mutator after read-only preflight.
    /// The durable outcome marker is committed before the caller may spawn Git.
    pub fn begin_workspace_git_effect(
        &mut self,
        command_id: &str,
        expected: &WorkspacePreparation,
        now: i64,
    ) -> Result<WorkspaceGitReservation> {
        self.ensure_writer_healthy()?;
        let result = self.begin_workspace_git_effect_inner(command_id, expected, now);
        self.fault_on_persistence_error(&result);
        result
    }

    fn begin_workspace_git_effect_inner(
        &mut self,
        command_id: &str,
        expected: &WorkspacePreparation,
        now: i64,
    ) -> Result<WorkspaceGitReservation> {
        require_nonempty(command_id, "command_id")?;
        validate_ulid(command_id)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let agent_num = parse_agent_id(&expected.agent)?;
        let workspace = load_workspace(&tx, agent_num)?
            .ok_or_else(|| Error::new("not_found", "workspace preparation was not found"))?;
        if workspace != *expected {
            return Err(Error::new(
                "bad_state",
                "workspace preparation changed before native Git reservation",
            ));
        }
        let command = load_command_record(&tx, command_id)?
            .ok_or_else(|| Error::new("not_found", "workspace Git command was not found"))?;
        if command.target != Some(agent_num) {
            return Err(Error::new(
                "source_integrity",
                "workspace Git command targets another Agent",
            ));
        }
        let outcome = workspace.outcome.as_ref();
        let stage = outcome
            .and_then(|value| value.get("stage"))
            .and_then(Value::as_str);
        let reuse = outcome
            .and_then(|value| value.get("reuse"))
            .and_then(Value::as_bool)
            == Some(true);
        let resume_previous = outcome
            .and_then(|value| value.get("resume_previous"))
            .is_some();
        if reuse && resume_previous {
            return Err(Error::new(
                "bad_state",
                "retained Resume readback does not start a native Git mutation",
            ));
        }
        let creation_identity = stage == Some("creation")
            && workspace.start_command_id == command_id
            && command.command.verb == "launch"
            && !reuse;
        let removal_identity = stage == Some("removal")
            && outcome
                .and_then(|value| value.get("command_id"))
                .and_then(Value::as_str)
                == Some(command_id)
            && command.command.verb == "worktree.remove";
        if !creation_identity && !removal_identity {
            return Err(Error::new(
                "bad_state",
                "native Git reservation requires its exact workspace create or remove",
            ));
        }

        if removal_identity {
            let agent = load_agent_by_num(&tx, agent_num)?
                .ok_or_else(|| Error::new("not_found", "workspace owner was not found"))?;
            if command.command.generation != Some(agent.generation)
                || workspace.target_generation != agent.generation
                || !agent.is_terminal()
                || agent.active_turn.is_some()
                || command.expected_turn_id.is_some()
                || command.command.request_id.is_some()
            {
                return Err(Error::new(
                    "stale_generation",
                    "worktree removal no longer targets its terminal workspace generation",
                ));
            }
        } else {
            if preparation_command(&tx, &workspace)?.0.command.id != command_id {
                return Err(Error::new(
                    "source_integrity",
                    "workspace start command changed before native Git reservation",
                ));
            }
        }

        let native_phase = matches!(
            workspace.phase,
            WorkspacePhase::Creating | WorkspacePhase::Removing | WorkspacePhase::Unknown
        );
        match outcome
            .and_then(|value| value.get("native_git_write_dispatched"))
            .and_then(Value::as_bool)
        {
            Some(true) if native_phase => {
                commit_transaction(tx, &self.write_faulted)?;
                return Ok(WorkspaceGitReservation::MayHaveBegun);
            }
            None if native_phase => {
                // Old in-flight rows have no explicit no-dispatch proof.
                commit_transaction(tx, &self.write_faulted)?;
                return Ok(WorkspaceGitReservation::MayHaveBegun);
            }
            Some(false) => {}
            Some(true) | None => {
                return Err(Error::new(
                    "bad_state",
                    "workspace Git marker is outside a native creation/removal phase",
                ));
            }
        }

        let eligible = if creation_identity {
            workspace.phase == WorkspacePhase::Creating
                && matches!(command.state, CommandState::Accepted | CommandState::Queued)
        } else {
            workspace.phase == WorkspacePhase::Removing && command.state == CommandState::Dispatched
        };
        if !eligible {
            return Err(Error::new(
                "bad_state",
                "workspace Git command is not at its native mutation boundary",
            ));
        }
        projects::validate_project_command_dispatch_tx(&tx, &command)?;
        if repo_git_write_blocked_tx(&tx, command_id, &workspace.repo_common_dir)? {
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(WorkspaceGitReservation::Deferred);
        }

        let old = workspace.clone();
        let mut workspace = workspace;
        let marker = workspace
            .outcome
            .as_mut()
            .and_then(Value::as_object_mut)
            .ok_or_else(|| Error::new("source_integrity", "workspace outcome is not an object"))?;
        marker.insert("native_git_write_dispatched".to_owned(), Value::Bool(true));
        workspace.updated_at = now;
        persist_workspace(&tx, &old, &workspace)?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok(WorkspaceGitReservation::Reserved)
    }

    pub fn settle_workspace_creation(
        &mut self,
        command: &str,
        target_generation: i64,
        outcome: WorkspaceCreationOutcome,
        now: i64,
    ) -> Result<Change<WorkspacePreparation>> {
        self.workspace_action(
            command,
            target_generation,
            WorkspaceAction::Creation(outcome),
            now,
        )
    }

    /// Withdraw an exact managed resume before native dispatch. This remains
    /// available after Project authority changes because it removes an effect
    /// already admitted; it does not authorize a new effect.
    pub fn reject_workspace_resume(
        &mut self,
        command_id: &str,
        reason: &str,
        now: i64,
    ) -> Result<Change<Command>> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            require_nonempty(command_id, "command_id")?;
            require_nonempty(reason, "reason")?;
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let record = load_command_record(&tx, command_id)?
                .ok_or_else(|| Error::new("not_found", "workspace resume was not found"))?;
            if record.command.verb != "resume"
                || !matches!(record.state, CommandState::Accepted | CommandState::Queued)
            {
                return Err(Error::new(
                    "bad_state",
                    "only an accepted or queued workspace resume can be withdrawn",
                ));
            }
            let agent_num = record
                .target
                .ok_or_else(|| Error::new("source_integrity", "workspace resume has no Agent"))?;
            let old_generation = record.command.generation.ok_or_else(|| {
                Error::new(
                    "source_integrity",
                    "workspace resume has no retained generation",
                )
            })?;
            let next_generation = old_generation
                .checked_add(1)
                .ok_or_else(|| Error::new("stale_generation", "resume generation overflow"))?;
            let workspace = load_workspace(&tx, agent_num)?.ok_or_else(|| {
                Error::new("stale_generation", "resume workspace marker is absent")
            })?;
            if workspace.start_command_id != command_id
                || workspace.target_generation != next_generation
            {
                return Err(Error::new(
                    "stale_generation",
                    "resume no longer owns the exact next-generation workspace marker",
                ));
            }
            let (prepared, agent) = preparation_command(&tx, &workspace)?;
            if prepared.command.id != command_id
                || prepared.state != record.state
                || agent.generation != old_generation
                || !agent.is_terminal()
                || agent.active_turn.is_some()
                || has_pending_resume_tx(&tx, agent_num, old_generation, command_id)?
                || has_in_flight_stop_tx(&tx, agent_num, old_generation)?
            {
                return Err(Error::new(
                    "bad_state",
                    "resume withdrawal requires its retained terminal generation without uncertain custody",
                ));
            }

            let outcome = json!({
                "stage": "workspace_resume_rejected",
                "reason": reason,
                "native_dispatched": false,
                "target_generation": next_generation,
            });
            if encode_json(&outcome)?.len() > MAX_EVENT_BYTES {
                return Err(Error::new(
                    "frame_too_large",
                    "workspace resume rejection exceeds 64 KiB",
                ));
            }
            update_command_state(
                &tx,
                command_id,
                CommandState::Rejected,
                Some(&outcome),
                None,
                now,
            )?;
            let mut events = vec![insert_daemon_command_event(
                &tx,
                now,
                "command.rejected",
                Some(agent_num),
                Some(old_generation),
                command_id,
                json!({"command_id": command_id, "outcome": outcome}),
            )?];
            let restored = cancel_unstarted_resume_input_tx(
                &tx,
                agent_num,
                command_id,
                reason,
                now,
                &mut events,
                false,
            )?;
            if restored.is_none() {
                return Err(Error::new(
                    "source_integrity",
                    "managed resume withdrawal lost its workspace marker",
                ));
            }
            let command = load_command(&tx, command_id)?
                .ok_or_else(|| Error::new("internal", "withdrawn resume disappeared"))?;
            commit_transaction(tx, &self.write_faulted)?;
            Ok(Change {
                value: command,
                events,
            })
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn begin_workspace_setup(
        &mut self,
        command: &str,
        target_generation: i64,
        index: usize,
        now: i64,
    ) -> Result<Change<WorkspacePreparation>> {
        self.workspace_action(
            command,
            target_generation,
            WorkspaceAction::Begin(index),
            now,
        )
    }

    pub fn settle_workspace_setup(
        &mut self,
        command: &str,
        target_generation: i64,
        index: usize,
        outcome: WorkspaceSetupOutcome,
        now: i64,
    ) -> Result<Change<WorkspacePreparation>> {
        self.workspace_action(
            command,
            target_generation,
            WorkspaceAction::Setup(index, outcome),
            now,
        )
    }

    pub fn request_workspace_cancel(
        &mut self,
        stop_command: &str,
        start_command: &str,
        target_generation: i64,
        now: i64,
    ) -> Result<Change<WorkspacePreparation>> {
        self.workspace_action(
            start_command,
            target_generation,
            WorkspaceAction::CancelRequest(stop_command.to_owned()),
            now,
        )
    }

    pub fn settle_workspace_cancel(
        &mut self,
        stop_command: &str,
        start_command: &str,
        target_generation: i64,
        outcome: WorkspaceCancelOutcome,
        now: i64,
    ) -> Result<Change<WorkspacePreparation>> {
        self.workspace_action(
            start_command,
            target_generation,
            WorkspaceAction::Cancel(stop_command.to_owned(), outcome),
            now,
        )
    }

    pub fn settle_workspace_removal(
        &mut self,
        command: &str,
        target_generation: i64,
        outcome: WorkspaceRemovalOutcome,
        now: i64,
    ) -> Result<Change<WorkspacePreparation>> {
        self.workspace_action(
            command,
            target_generation,
            WorkspaceAction::Removal(outcome),
            now,
        )
    }

    fn workspace_action(
        &mut self,
        command: &str,
        target: i64,
        action: WorkspaceAction,
        now: i64,
    ) -> Result<Change<WorkspacePreparation>> {
        self.ensure_writer_healthy()?;
        let result = self.workspace_action_inner(command, target, action, now);
        self.fault_on_persistence_error(&result);
        result
    }

    fn workspace_action_inner(
        &mut self,
        command_id: &str,
        target: i64,
        action: WorkspaceAction,
        now: i64,
    ) -> Result<Change<WorkspacePreparation>> {
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let command = load_command_record(&tx, command_id)?
            .ok_or_else(|| Error::new("not_found", "workspace command was not found"))?;
        let agent_num = command
            .target
            .ok_or_else(|| Error::new("invalid", "workspace command has no Agent"))?;
        let mut workspace = load_workspace(&tx, agent_num)?
            .ok_or_else(|| Error::new("not_found", "workspace preparation was not found"))?;
        let old = workspace.clone();
        let removal = matches!(&action, WorkspaceAction::Removal(_));
        if workspace.target_generation != target
            || (!removal && workspace.start_command_id != command_id)
        {
            return Err(Error::new(
                "stale_generation",
                "workspace command or target generation changed",
            ));
        }
        let reuse = workspace
            .outcome
            .as_ref()
            .and_then(|value| value.get("reuse"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let resume_previous = workspace
            .outcome
            .as_ref()
            .and_then(|value| value.get("resume_previous"))
            .cloned();
        let mut marker = match &action {
            WorkspaceAction::Creation(outcome) => {
                json!({"stage": "creation", "reuse": reuse, "result": outcome})
            }
            WorkspaceAction::Begin(index) => json!({"stage": "setup", "index": index}),
            WorkspaceAction::Setup(index, outcome) => {
                json!({"stage": "setup", "index": index, "result": outcome})
            }
            WorkspaceAction::CancelRequest(stop) => {
                json!({"stage": "cancel", "stop_command_id": stop, "cancel_requested": true})
            }
            WorkspaceAction::Cancel(stop, outcome) => {
                json!({"stage": "cancel", "reuse": reuse, "stop_command_id": stop, "cancel_requested": true, "result": outcome})
            }
            WorkspaceAction::Removal(outcome) => {
                json!({"stage": "removal", "command_id": command_id, "result": outcome,
                    "previous_phase": workspace.outcome.as_ref().and_then(|value| value.get("previous_phase"))})
            }
        };
        preserve_native_git_write_marker(old.outcome.as_ref(), &mut marker);
        if reuse
            && matches!(
                &action,
                WorkspaceAction::Creation(_) | WorkspaceAction::Cancel(_, _)
            )
            && let Some(previous) = resume_previous
        {
            marker["resume_previous"] = previous;
        }
        if encode_json(&marker)?.len() > MAX_EVENT_BYTES {
            return Err(Error::new(
                "frame_too_large",
                "workspace outcome exceeds its bound",
            ));
        }
        if workspace.outcome.as_ref() == Some(&marker)
            && !matches!(
                &action,
                WorkspaceAction::Begin(_) | WorkspaceAction::CancelRequest(_)
            )
        {
            let mut events = Vec::new();
            if removal && workspace.phase == WorkspacePhase::Removed {
                close_finished_workspace_process_source(&tx, agent_num, target, now, &mut events)?;
            }
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(Change {
                value: workspace,
                events,
            });
        }
        let mut events = Vec::new();
        if let WorkspaceAction::CancelRequest(stop) = &action {
            request_workspace_cancel_tx(&tx, &mut workspace, stop, now, &mut events)?;
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(Change {
                value: workspace,
                events,
            });
        }
        let mut result_state = None;
        let mut stop_result = None;
        if removal {
            let agent = load_agent_by_num(&tx, agent_num)?
                .ok_or_else(|| Error::new("not_found", "workspace owner was not found"))?;
            if command.command.verb != "worktree.remove"
                || command.command.generation != Some(agent.generation)
                || !agent.is_terminal()
                || !matches!(
                    command.state,
                    CommandState::Dispatched | CommandState::Unknown
                )
                || !matches!(
                    workspace.phase,
                    WorkspacePhase::Removing | WorkspacePhase::Unknown
                )
                || workspace
                    .outcome
                    .as_ref()
                    .and_then(|value| value.get("stage"))
                    .and_then(Value::as_str)
                    != Some("removal")
                || workspace
                    .outcome
                    .as_ref()
                    .and_then(|value| value.get("command_id"))
                    .and_then(Value::as_str)
                    != Some(command_id)
            {
                return Err(Error::new(
                    "bad_state",
                    "removal settlement requires its exact removing/unknown marker",
                ));
            }
        } else {
            preparation_command(&tx, &workspace)?;
            if !matches!(
                command.state,
                CommandState::Accepted | CommandState::Queued | CommandState::Unknown
            ) {
                return Err(Error::new(
                    "bad_state",
                    "native start was already dispatched or settled",
                ));
            }
            if workspace_cancel_requested(&workspace)
                && !matches!(&action, WorkspaceAction::Cancel(_, _))
            {
                return Err(Error::new("bad_state", "workspace cancellation is pending"));
            }
        }
        match action {
            WorkspaceAction::Creation(outcome) => {
                let ready_resume_unknown = workspace.phase == WorkspacePhase::Ready
                    && command.command.verb == "resume"
                    && reuse
                    && workspace.setup_index == workspace.frozen_setup.len()
                    && matches!(command.state, CommandState::Accepted | CommandState::Queued)
                    && matches!(&outcome, WorkspaceCreationOutcome::Unknown { .. });
                if !(matches!(
                    workspace.phase,
                    WorkspacePhase::Creating | WorkspacePhase::Unknown
                ) || ready_resume_unknown)
                    || workspace
                        .outcome
                        .as_ref()
                        .and_then(|value| value.get("stage"))
                        .and_then(Value::as_str)
                        != Some("creation")
                    || workspace.setup_index
                        != if reuse {
                            workspace.frozen_setup.len()
                        } else {
                            0
                        }
                {
                    return Err(Error::new(
                        "bad_state",
                        "creation readback cannot resolve an unknown setup/removal",
                    ));
                }
                match outcome {
                    WorkspaceCreationOutcome::Verified {
                        repo_path,
                        repo_common_dir,
                        worktree_path,
                        branch,
                        head,
                    } => {
                        validate_git_oid(&head)?;
                        if repo_path != workspace.repo_path
                            || repo_common_dir != workspace.repo_common_dir
                            || worktree_path != workspace.worktree_path
                            || branch != workspace.requested_branch
                            || (!reuse && head != workspace.base_oid)
                        {
                            return Err(Error::new(
                                "source_integrity",
                                "worktree readback does not match its frozen identity",
                            ));
                        }
                        workspace.phase = if workspace.setup_index == workspace.frozen_setup.len() {
                            WorkspacePhase::Ready
                        } else {
                            WorkspacePhase::SetupReady
                        };
                        if command.state == CommandState::Unknown {
                            if !core::workspace_preparation_transition_allowed(
                                command.state,
                                CommandState::Accepted,
                            ) {
                                return Err(Error::new(
                                    "bad_state",
                                    "workspace creation readback transition is not allowed",
                                ));
                            }
                            write_command_state(
                                &tx,
                                command_id,
                                CommandState::Accepted,
                                None,
                                None,
                                now,
                            )?;
                            events.push(insert_daemon_command_event(
                                &tx,
                                now,
                                "command.accepted",
                                Some(agent_num),
                                command.command.generation,
                                command_id,
                                json!({"command_id": command_id, "reason": "exact_workspace_creation_readback"}),
                            )?);
                        }
                    }
                    WorkspaceCreationOutcome::Failed { .. } => {
                        if old.phase == WorkspacePhase::Unknown {
                            return Err(Error::new(
                                "unknown",
                                "absent or partial creation readback remains unknown",
                            ));
                        }
                        workspace.phase = WorkspacePhase::Failed;
                        result_state = Some(CommandState::Failed);
                    }
                    WorkspaceCreationOutcome::Unknown { .. } => {
                        workspace.phase = WorkspacePhase::Unknown;
                        result_state = Some(CommandState::Unknown);
                    }
                }
            }
            WorkspaceAction::Begin(index) => {
                if workspace.phase == WorkspacePhase::SetupRunning {
                    return Err(Error::new(
                        "unknown",
                        "setup entry already has a running marker; never spawn it again",
                    ));
                }
                if workspace.phase != WorkspacePhase::SetupReady
                    || index != workspace.setup_index
                    || index >= workspace.frozen_setup.len()
                    || !matches!(command.state, CommandState::Accepted | CommandState::Queued)
                {
                    return Err(Error::new(
                        "bad_state",
                        "setup requires the exact next unperformed entry",
                    ));
                }
                workspace.phase = WorkspacePhase::SetupRunning;
            }
            WorkspaceAction::Setup(index, outcome) => {
                let not_started = matches!(&outcome, WorkspaceSetupOutcome::NotStarted { .. });
                if !(workspace.phase == WorkspacePhase::SetupRunning
                    || not_started && workspace.phase == WorkspacePhase::SetupReady)
                    || index != workspace.setup_index
                    || index >= workspace.frozen_setup.len()
                    || !matches!(command.state, CommandState::Accepted | CommandState::Queued)
                {
                    return Err(Error::new(
                        "unknown",
                        "setup result does not match its live running marker",
                    ));
                }
                match outcome {
                    WorkspaceSetupOutcome::Succeeded => {
                        workspace.setup_index += 1;
                        workspace.phase = if workspace.setup_index == workspace.frozen_setup.len() {
                            WorkspacePhase::Ready
                        } else {
                            WorkspacePhase::SetupReady
                        };
                    }
                    WorkspaceSetupOutcome::Failed { exit_code: 0 } => {
                        return Err(Error::new("invalid", "zero exit code must use succeeded"));
                    }
                    WorkspaceSetupOutcome::Failed { .. }
                    | WorkspaceSetupOutcome::NotStarted { .. } => {
                        workspace.phase = WorkspacePhase::Failed;
                        result_state = Some(CommandState::Failed);
                    }
                    WorkspaceSetupOutcome::Unknown { .. } => {
                        workspace.phase = WorkspacePhase::Unknown;
                        result_state = Some(CommandState::Unknown);
                    }
                }
            }
            WorkspaceAction::Cancel(stop_id, outcome) => {
                let stop = load_command_record(&tx, &stop_id)?
                    .ok_or_else(|| Error::new("not_found", "stop command was not found"))?;
                if !workspace_cancel_requested(&workspace)
                    || workspace
                        .outcome
                        .as_ref()
                        .and_then(|value| value.get("stop_command_id"))
                        .and_then(Value::as_str)
                        != Some(stop_id.as_str())
                    || stop.command.verb != "stop"
                    || stop.target != Some(agent_num)
                    || stop.command.generation != command.command.generation
                    || !matches!(stop.state, CommandState::Dispatched | CommandState::Unknown)
                {
                    return Err(Error::new(
                        "source_integrity",
                        "cancellation is not bound to its dispatched stop command",
                    ));
                }
                let stopped = matches!(outcome, WorkspaceCancelOutcome::Stopped);
                workspace.phase = if stopped {
                    WorkspacePhase::Failed
                } else {
                    WorkspacePhase::Unknown
                };
                result_state = Some(if stopped {
                    CommandState::Cancelled
                } else {
                    CommandState::Unknown
                });
                stop_result = Some((
                    stop_id,
                    if stopped {
                        CommandState::Applied
                    } else {
                        CommandState::Unknown
                    },
                ));
            }
            WorkspaceAction::Removal(outcome) => {
                let state = match outcome {
                    WorkspaceRemovalOutcome::Removed {
                        repo_common_dir,
                        worktree_path,
                        branch,
                        branch_retained,
                    } => {
                        if repo_common_dir != workspace.repo_common_dir
                            || worktree_path != workspace.worktree_path
                            || branch != workspace.requested_branch
                            || !branch_retained
                        {
                            return Err(Error::new(
                                "source_integrity",
                                "removal readback must prove exact absence and retain the branch",
                            ));
                        }
                        workspace.phase = WorkspacePhase::Removed;
                        db!(tx.execute(
                            "DELETE FROM agent_edits WHERE workspace = ?1",
                            [&workspace.worktree_path],
                        ));
                        CommandState::Applied
                    }
                    WorkspaceRemovalOutcome::Failed { .. } => {
                        let previous = workspace
                            .outcome
                            .as_ref()
                            .and_then(|value| value.get("previous_phase"))
                            .cloned()
                            .ok_or_else(|| {
                                Error::new(
                                    "source_integrity",
                                    "removal has no previous preparation phase",
                                )
                            })?;
                        let previous: WorkspacePhase =
                            serde_json::from_value(previous).map_err(|_| {
                                Error::new("source_integrity", "invalid previous removal phase")
                            })?;
                        if !matches!(
                            previous,
                            WorkspacePhase::Ready
                                | WorkspacePhase::Failed
                                | WorkspacePhase::Unknown
                        ) {
                            return Err(Error::new(
                                "source_integrity",
                                "removal cannot restore an active or removed preparation",
                            ));
                        }
                        workspace.phase = previous;
                        CommandState::Failed
                    }
                    WorkspaceRemovalOutcome::Unknown { .. } => {
                        workspace.phase = WorkspacePhase::Unknown;
                        CommandState::Unknown
                    }
                };
                update_command_state(&tx, command_id, state, Some(&marker), None, now)?;
                events.push(insert_daemon_command_event(
                    &tx,
                    now,
                    &format!("command.{}", command_state_name(state)),
                    Some(agent_num),
                    command.command.generation,
                    command_id,
                    json!({"command_id": command_id, "outcome": marker}),
                )?);
            }
            WorkspaceAction::CancelRequest(_) => unreachable!(),
        }
        workspace.outcome = Some(marker);
        workspace.updated_at = now;
        let mut restored_workspace = None;
        if let Some(state) = result_state {
            restored_workspace =
                settle_preparation_command(&tx, &workspace, state, now, &mut events)?;
        }
        if let Some((stop_id, state)) = stop_result {
            let outcome = json!({"stage": "workspace_cancel", "start_command_id": workspace.start_command_id, "target_generation": target, "detail": workspace.outcome});
            update_command_state(&tx, &stop_id, state, Some(&outcome), None, now)?;
            events.push(insert_daemon_command_event(
                &tx,
                now,
                &format!("command.{}", command_state_name(state)),
                Some(agent_num),
                command.command.generation,
                &stop_id,
                json!({"command_id": stop_id, "outcome": outcome}),
            )?);
        }
        if let Some(restored) = restored_workspace {
            workspace = restored;
        } else {
            persist_workspace(&tx, &old, &workspace)?;
            events.push(workspace_event(&tx, &workspace, now)?);
        }
        if removal && workspace.phase == WorkspacePhase::Removed {
            close_finished_workspace_process_source(&tx, agent_num, target, now, &mut events)?;
        }
        commit_transaction(tx, &self.write_faulted)?;
        Ok(Change {
            value: workspace,
            events,
        })
    }
}

pub(super) fn pending_workspace_for_stop(
    connection: &Connection,
    agent: &Agent,
) -> Result<Option<WorkspacePreparation>> {
    let Some(workspace) = load_workspace(connection, parse_agent_id(&agent.id)?)? else {
        return Ok(None);
    };
    if agent.generation.checked_add(1) != Some(workspace.target_generation)
        || matches!(
            workspace.phase,
            WorkspacePhase::Removing | WorkspacePhase::Removed | WorkspacePhase::Failed
        )
    {
        return Ok(None);
    }
    let start = load_command_record(connection, &workspace.start_command_id)?
        .ok_or_else(|| Error::new("source_integrity", "workspace start disappeared"))?;
    Ok(matches!(
        start.state,
        CommandState::Accepted | CommandState::Queued | CommandState::Unknown
    )
    .then_some(workspace))
}

pub(super) fn request_workspace_cancel_tx(
    tx: &Transaction<'_>,
    workspace: &mut WorkspacePreparation,
    stop_id: &str,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    if matches!(
        workspace.phase,
        WorkspacePhase::Failed | WorkspacePhase::Removing | WorkspacePhase::Removed
    ) || workspace
        .outcome
        .as_ref()
        .and_then(|value| value.get("stage"))
        .and_then(Value::as_str)
        == Some("removal")
    {
        return Err(Error::new(
            "bad_state",
            "workspace cancellation cannot replace removal or terminal preparation",
        ));
    }
    let (start, agent) = preparation_command(tx, workspace)?;
    let stop = load_command_record(tx, stop_id)?
        .ok_or_else(|| Error::new("not_found", "stop command was not found"))?;
    projects::validate_project_command_dispatch_tx(tx, &stop)?;
    if stop.command.verb != "stop"
        || stop.target != start.target
        || stop.command.generation != Some(agent.generation)
        || stop.expected_turn_id.is_some()
        || !matches!(
            start.state,
            CommandState::Accepted | CommandState::Queued | CommandState::Unknown
        )
        || !matches!(stop.state, CommandState::Accepted | CommandState::Queued)
    {
        return Err(Error::new(
            "bad_state",
            "preparation cancellation requires its exact undispatched stop",
        ));
    }
    if workspace_cancel_requested(workspace) {
        return Err(Error::new(
            "unknown",
            "cancellation was already requested; read back instead of replaying stop",
        ));
    }
    if rules::gate_rule_action_dispatch_tx(tx, stop_id, now, events)?
        != rules::RuleActionDispatchGate::Ready
    {
        return Ok(());
    }
    let old = workspace.clone();
    let outcome = workspace.outcome.get_or_insert_with(|| json!({}));
    outcome["cancel_requested"] = json!(true);
    outcome["stop_command_id"] = json!(stop_id);
    workspace.updated_at = now;
    persist_workspace(tx, &old, workspace)?;
    let result = json!({"stage": "workspace_cancel", "start_command_id": workspace.start_command_id, "target_generation": workspace.target_generation});
    update_command_state(
        tx,
        stop_id,
        CommandState::Dispatched,
        Some(&result),
        None,
        now,
    )?;
    events.push(workspace_event(tx, workspace, now)?);
    events.push(insert_daemon_command_event(
        tx,
        now,
        "command.dispatched",
        start.target,
        Some(agent.generation),
        stop_id,
        json!({"command_id": stop_id, "outcome": result}),
    )?);
    Ok(())
}

pub(super) fn recover_workspaces(
    tx: &Transaction<'_>,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    let mut statement = tx.prepare(&format!(
        "SELECT {WORKSPACE_COLUMNS} FROM workspaces WHERE phase IN ('creating','setup_running','removing')
         OR (json_extract(outcome, '$.cancel_requested') = 1 AND phase IN ('setup_ready','ready'))"
    )).map_err(sql_error)?;
    let rows = statement
        .query_map([], workspace_from_row)
        .map_err(sql_error)?;
    let workspaces = rows
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(sql_error)?;
    drop(statement);
    for mut workspace in workspaces {
        let old = workspace.clone();
        let removing = workspace.phase == WorkspacePhase::Removing;
        workspace.phase = WorkspacePhase::Unknown;
        let outcome = workspace.outcome.get_or_insert_with(|| json!({}));
        outcome["reason"] = json!("daemon_restart");
        workspace.updated_at = now;
        if !removing {
            let _ = settle_preparation_command(tx, &workspace, CommandState::Unknown, now, events)?;
        }
        persist_workspace(tx, &old, &workspace)?;
        events.push(workspace_event(tx, &workspace, now)?);
    }
    Ok(())
}
