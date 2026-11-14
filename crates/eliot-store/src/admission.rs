use super::*;

/// Local topic reserved for the internal attention producer. Public `emit`
/// commands cannot use it; only the internal `attention.emit` verb admits it.
pub(super) const ATTENTION_RESERVED_TOPIC: &str = "agent.attention";

pub(super) fn command_semantic_payload(request: &SubmitCommand, args: Value) -> Result<Value> {
    let mut payload = json!({
        "target": request.target,
        "verb": request.verb,
        "args": args,
        "expected_generation": request.expected_generation,
        "expected_turn_id": request.expected_turn_id,
    });
    // Optional bindings join the digest only when present, so legacy
    // requests keep their retained bytes and bindings.
    if let Some(selector) = request.selector.as_deref() {
        payload["selector"] = Value::String(selector.to_owned());
    }
    if request.probe {
        payload["probe"] = Value::Bool(true);
    }
    if let Some(request_id) = request.request_id.as_deref() {
        payload["request_id"] = Value::String(request_id.to_owned());
    }
    if encode_json(&payload)?.len() > MAX_FRAME_BYTES {
        return Err(Error::new(
            "frame_too_large",
            "command semantic request exceeds its bound",
        ));
    }
    Ok(payload)
}

pub(super) fn command_payload_digest(request: &SubmitCommand) -> Result<Vec<u8>> {
    if !request.args.is_object() {
        return Err(Error::new("invalid", "command args must be a JSON object"));
    }
    if request.verb != "attach" && encode_json(request)?.len() > MAX_FRAME_BYTES {
        return Err(Error::new(
            "frame_too_large",
            "command request exceeds 1 MiB",
        ));
    }
    validate_ulid(&request.id)?;

    let mut semantic_args = if request.verb == "git.fetch" {
        serde_json::from_value::<core::api::FrozenGitFetch>(request.args.clone())
            .map_err(|_| Error::new("invalid", "git.fetch args must be a frozen intent"))?
            .binding_args()?
    } else {
        request.args.clone()
    };
    if request.verb == "attach" {
        semantic_args
            .as_object_mut()
            .ok_or_else(|| Error::new("invalid", "attach args must be an object"))?
            .remove("profile_snapshot");
    }
    let semantic_payload = command_semantic_payload(request, semantic_args)?;
    canonical::digest(&semantic_payload)
}

pub(super) fn replay_command_binding(
    connection: &Connection,
    principal: &str,
    request: &SubmitCommand,
    payload_digest: &[u8],
) -> Result<Option<Admission>> {
    let Some((old_principal, old_method, old_digest)) = command_binding(connection, &request.id)?
    else {
        return Ok(None);
    };
    if old_principal != principal || old_method != COMMAND_METHOD || old_digest != payload_digest {
        return Err(Error::new(
            "id_conflict",
            "command id is already bound to another principal, method, or payload",
        ));
    }
    let command = load_command(connection, &request.id)?
        .ok_or_else(|| Error::new("internal", "command binding exists without its command row"))?;
    let linked_command = load_linked_command(connection, &request.id)?;
    Ok(Some(Admission {
        command,
        linked_command,
        events: Vec::new(),
        is_new: false,
    }))
}

/// Admit one tree-selector expansion: snapshot members once, admit one
/// concrete child per member with the ordinary per-target gates, then the
/// Store-only applied parent. Any failure rejects the whole expansion.
fn submit_selector_tx(
    tx: &Transaction<'_>,
    principal: &str,
    request: SubmitCommand,
    payload_digest: &[u8],
    now: i64,
    derived_rule: bool,
    limits: core::api::HierarchyLimits,
) -> Result<Admission> {
    let selector = request.selector.as_deref().unwrap_or_default();
    if derived_rule {
        return Err(Error::new(
            "unsupported",
            "rule actions do not support selectors",
        ));
    }
    if request.target.is_some() {
        return Err(Error::new("invalid", "a selector command takes no target"));
    }
    if request.expected_generation.is_some()
        || request.expected_turn_id.is_some()
        || request.request_id.is_some()
        || request.probe
    {
        return Err(Error::new(
            "invalid",
            "a selector command takes no generation, turn, request, or probe binding",
        ));
    }
    if !matches!(request.verb.as_str(), "send" | "stop") {
        return Err(Error::new(
            "unsupported",
            "selectors are supported only for send and stop",
        ));
    }
    if selector.trim().is_empty() {
        return Err(Error::new("invalid", "selector must not be empty"));
    }
    hierarchy::admit_selector_expansion(tx, principal, &request, payload_digest, limits, now)
}

/// Shape bounds for `configure` values, mirroring the profile freeze
/// (`MAX_MODEL_LEN`, `MAX_EFFORT_LEN` in the daemon config): nonempty,
/// bounded, no control characters. Native catalog support is enforced at
/// the next turn, never here, and catalog failures never select another
/// model or account.
const MAX_CONFIGURE_MODEL_LEN: usize = 256;
const MAX_CONFIGURE_EFFORT_LEN: usize = 128;

/// Authority-adjacent profile fields a `configure` must never touch.
/// Named here they are rejected outright (`not_allowed`); any other
/// unknown field is `unsupported`.
fn is_configure_authority_field(key: &str) -> bool {
    matches!(
        key,
        "permissions"
            | "approval"
            | "mcp"
            | "mcp_allow"
            | "env"
            | "literal_env"
            | "pass_env"
            | "inherit_env"
            | "secret_env_names"
            | "route"
            | "route_binding"
            | "account"
            | "provider"
            | "proxy"
            | "billing"
            | "catalog"
            | "quota"
            | "capacity"
            | "capacity_scopes"
            | "workspace"
            | "worktree"
            | "base"
            | "harness"
            | "profile"
            | "control"
            | "project"
            | "credential"
            | "token"
    )
}

fn configure_value(
    args: &Map<String, Value>,
    field: &str,
    max_len: usize,
) -> Result<Option<String>> {
    match args.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => {
            if value.trim().is_empty()
                || value.len() > max_len
                || value.chars().any(char::is_control)
            {
                return Err(Error::new(
                    "invalid",
                    format!("configure {field} must be a nonempty string within its bound"),
                ));
            }
            Ok(Some(value.clone()))
        }
        Some(_) => Err(Error::new(
            "invalid",
            format!("configure {field} must be a string"),
        )),
    }
}

/// Resolve one `configure` request against a frozen profile snapshot into
/// the retained requested values, the effective values the next native
/// turn carries, and the application point. Only `model` and `effort` are
/// supported; authority fields are rejected outright and anything else is
/// unsupported. Codex carries both as per-turn `turn/start` overrides;
/// Muse carries effort per turn while a model change needs a new native
/// session (`restart_required`, refused with `unsupported`, never
/// restarted silently); adapters without a verified mechanism refuse the
/// whole command. Shared by admission and by the Router-side apply so
/// both agree on what was requested and what takes effect.
pub(super) fn resolve_configure(
    args: &Value,
    harness: &str,
    snapshot: &Value,
) -> Result<(Value, Value, &'static str)> {
    let args = args
        .as_object()
        .ok_or_else(|| Error::new("invalid", "configure args must be a JSON object"))?;
    for key in args.keys() {
        if is_configure_authority_field(key) {
            return Err(Error::new(
                "not_allowed",
                format!("configure cannot change {key}; authority fields are rejected outright"),
            ));
        }
        if !matches!(key.as_str(), "model" | "effort") {
            return Err(Error::new(
                "unsupported",
                format!("configure does not support {key}"),
            ));
        }
    }
    let model = configure_value(args, "model", MAX_CONFIGURE_MODEL_LEN)?;
    let effort = configure_value(args, "effort", MAX_CONFIGURE_EFFORT_LEN)?;
    if model.is_none() && effort.is_none() {
        return Err(Error::new(
            "invalid",
            "configure requires at least one of model or effort",
        ));
    }
    match harness {
        "codex" => {}
        "muse" => {
            if model.is_some() {
                return Err(Error::new(
                    "unsupported",
                    "Muse model changes require a new native session (restart_required)",
                ));
            }
        }
        _ => {
            return Err(Error::new(
                "unsupported",
                format!("configure is not supported for the {harness} harness"),
            ));
        }
    }
    let original_model = snapshot
        .get("model")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let original_effort = snapshot
        .get("effort")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let requested = json!({"model": model, "effort": effort});
    let effective = json!({
        "model": model.clone().or(original_model),
        "effort": effort.clone().or(original_effort),
    });
    Ok((requested, effective, "next_turn"))
}

/// Admit one `send` to its exact target, shared by the single-target path
/// and selector members (linked to their expansion parent afterwards).
pub(super) fn admit_send_member(
    tx: &Transaction<'_>,
    principal: &str,
    request: &SubmitCommand,
    payload_digest: &[u8],
    agent: &Agent,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    let agent_num = parse_agent_id(&agent.id)?;
    if agent.generation == 0 || agent.state == AgentState::Created {
        return Err(Error::new(
            "bad_state",
            "dispatch launch before submitting Agent input",
        ));
    }
    if agent.is_terminal() {
        return Err(Error::new("bad_state", "cannot send to a terminal Agent"));
    }
    let text = required_string(&request.args, "text")?;
    check_input(&text)?;
    if request.args.get("context_items").is_some() {
        return Err(Error::new(
            "invalid",
            "send context_items are reserved for Store-claimed system context",
        ));
    }
    check_rpc_frame(
        "agent.command",
        &json!({
            "agent_id": agent.id,
            "generation": agent.generation,
            "command_id": request.id,
            "verb": "send",
            "args": request.args,
        }),
    )?;
    // Open-notice sends must carry explicit `notice_kind` mail/claim (no
    // default); mail then defers on busy/pending/unknown input. Claim
    // notices keep prior queuing; other sends skip this check entirely.
    let open_notice: Option<String> = tx
        .query_row(
            "SELECT open_notice_command FROM mail_notices WHERE recipient = ?1",
            params![agent_num],
            |row| row.get::<_, Option<String>>(0),
        )
        .optional()
        .map_err(sql_error)?
        .flatten();
    if open_notice.as_deref() == Some(request.id.as_str()) {
        let notice_kind = request
            .args
            .get("notice_kind")
            .and_then(Value::as_str)
            .filter(|kind| matches!(*kind, "mail" | "claim"))
            .ok_or_else(|| {
                Error::new(
                    "invalid",
                    "notice send requires an explicit notice_kind of mail or claim",
                )
            })?;
        if notice_kind == "mail"
            && (matches!(agent.state, AgentState::Busy | AgentState::Waiting)
                || has_pending_send_input(tx, agent_num, agent.generation)?
                || has_unknown_gate(tx, agent_num)?)
        {
            return Err(Error::new(
                "suppressed",
                "notice deferred while recipient input is pending",
            ));
        }
    }
    let ready = send_gate_open(tx, agent_num, agent.generation)?
        && earliest_pending_send(tx, agent_num, agent.generation)?.is_none();
    let state = if ready {
        CommandState::Accepted
    } else {
        CommandState::Queued
    };
    events.push(admit_command(
        tx,
        &CommandInsert {
            state,
            ..CommandInsert::admitted(
                principal,
                request,
                payload_digest,
                (agent_num, agent.generation),
                now,
            )
        },
        json!({"command_id": request.id, "verb": "send"}),
    )?);
    if let Some(event) = provider_holds::admit_send_tx(tx, agent, &request.id, request.probe, now)?
    {
        events.push(event);
    }
    Ok(())
}

/// Admit one `stop` to its exact target, shared by the single-target path
/// and selector members (linked to their expansion parent afterwards).
pub(super) fn admit_stop_member(
    tx: &Transaction<'_>,
    principal: &str,
    request: &SubmitCommand,
    payload_digest: &[u8],
    agent: &Agent,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    let agent_num = parse_agent_id(&agent.id)?;
    if request
        .args
        .get("force")
        .is_some_and(|value| !value.is_boolean())
    {
        return Err(Error::new("invalid", "stop force must be a boolean"));
    }
    check_rpc_frame(
        "agent.stop",
        &json!({
            "agent_id": agent.id,
            "generation": agent.generation,
            "command_id": request.id,
            "force": request.args.get("force").and_then(Value::as_bool).unwrap_or(false),
        }),
    )?;
    events.push(admit_command(
        tx,
        &CommandInsert {
            turn_id: request.expected_turn_id.as_deref(),
            ..CommandInsert::admitted(
                principal,
                request,
                payload_digest,
                (agent_num, agent.generation),
                now,
            )
        },
        json!({"command_id": request.id, "verb": "stop"}),
    )?);
    Ok(())
}

/// Opening gates shared by every verb: principal and shape bounds, payload
/// digest, same-ID replay, write conflicts, carried bindings, and ID age.
/// Returns the digest with any replayed admission.
fn submit_head_tx(
    tx: &Transaction<'_>,
    principal: &str,
    request: &SubmitCommand,
    now: i64,
    derived_rule: bool,
) -> Result<(Vec<u8>, Option<Admission>)> {
    require_nonempty(principal, "principal")?;
    if request.verb == "run" && !derived_rule {
        return Err(Error::new(
            "not_allowed",
            "run commands must be admitted by an event-rule firing",
        ));
    }
    if !request.args.is_object() {
        return Err(Error::new("invalid", "command args must be a JSON object"));
    }
    if request.probe && request.verb != "send" {
        return Err(Error::new(
            "bad_state",
            "provider probes are allowed only for SEND commands",
        ));
    }
    // Bound durable admission data here. Host checks the completed start
    // frame, including runtime environment and source state, before writing.
    let payload_digest = command_payload_digest(request)?;
    if let Some(admission) = replay_command_binding(tx, principal, request, &payload_digest)? {
        return Ok((payload_digest, Some(admission)));
    }
    if write_exists(tx, &request.id)? {
        return Err(Error::new(
            "id_conflict",
            "mutation id is already used by another write",
        ));
    }
    if matches!(request.verb.as_str(), "launch" | "attach")
        && request
            .args
            .get("profile_snapshot")
            .is_some_and(|snapshot| snapshot.get("native_store_binding").is_some())
        && !(request.verb == "launch"
            && hierarchy::admission_allows_carried_parent_binding(tx, principal, &request.args)?)
    {
        return Err(Error::new(
            "invalid",
            "native store binding is derived at launch dispatch",
        ));
    }
    // The internal attention verb carries deterministic mutation ids derived
    // from the retained loss evidence (like rule-derived ids), so its age is
    // the evidence's age, not a replay signal. Retained bindings still replay
    // before this check; see `commit_attention_event_applied`.
    if !derived_rule && request.verb != "attention.emit" {
        validate_id_age(&request.id, now)?;
    }
    Ok((payload_digest, None))
}

/// Target and preparation guards shared by every non-selector verb:
/// observer control for concrete targets and frozen workspace input.
fn submit_target_guards_tx(
    tx: &Transaction<'_>,
    request: &SubmitCommand,
    preparation: &Option<WorkspacePreparationInput>,
) -> Result<()> {
    if !matches!(request.verb.as_str(), "attach" | "detach")
        && let Some(target) = request.target.as_deref()
        && let Some(agent) = load_agent_by_num(tx, parse_agent_id(target)?)?
    {
        observer::require_controlled(&agent)?;
    }
    if let Some(input) = preparation {
        if !matches!(request.verb.as_str(), "launch" | "resume") {
            return Err(Error::new(
                "invalid",
                "workspace preparation requires launch or resume",
            ));
        }
        workspace::validate_workspace_input(input)?;
    } else if request.verb == "launch"
        && request
            .args
            .get("worktree")
            .is_some_and(|value| !value.is_null())
    {
        return Err(Error::new(
            "invalid",
            "worktree launch requires frozen preparation",
        ));
    }
    Ok(())
}

/// Admit one targetless `run`: no Agent bindings, frozen run intent only.
fn admit_run_tx(
    tx: &Transaction<'_>,
    principal: &str,
    request: &SubmitCommand,
    payload_digest: &[u8],
    preparation: &Option<WorkspacePreparationInput>,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<Option<String>> {
    if request.target.is_some()
        || request.expected_generation.is_some()
        || request.expected_turn_id.is_some()
        || request.request_id.is_some()
        || request.probe
        || preparation.is_some()
    {
        return Err(Error::new(
            "invalid",
            "run is a targetless command without Agent bindings",
        ));
    }
    scripts::decode_frozen_run(&request.args)?;
    events.push(admit_command(
        tx,
        &CommandInsert::targetless_admitted(principal, request, payload_digest, now),
        json!({"command_id": request.id, "verb": "run"}),
    )?);
    Ok(None)
}

/// Admit one targetless `git.fetch`: no Agent bindings, frozen intent only.
fn admit_git_fetch_tx(
    tx: &Transaction<'_>,
    principal: &str,
    request: &SubmitCommand,
    payload_digest: &[u8],
    preparation: &Option<WorkspacePreparationInput>,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<Option<String>> {
    if request.target.is_some()
        || request.expected_generation.is_some()
        || request.expected_turn_id.is_some()
        || request.request_id.is_some()
        || request.probe
        || preparation.is_some()
    {
        return Err(Error::new(
            "invalid",
            "git.fetch is a targetless command without Agent bindings",
        ));
    }
    let frozen = projects::decode_frozen_git_fetch(&request.args)?;
    projects::validate_git_fetch_intent_tx(tx, principal, &frozen)?;
    events.push(admit_command(
        tx,
        &CommandInsert::targetless_admitted(principal, request, payload_digest, now),
        json!({"command_id": request.id, "verb": "git.fetch"}),
    )?);
    Ok(None)
}

/// Admit one targetless `notify`/`emit` definition.
fn admit_notify_emit_tx(
    tx: &Transaction<'_>,
    principal: &str,
    request: &SubmitCommand,
    payload_digest: &[u8],
    preparation: &Option<WorkspacePreparationInput>,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<Option<String>> {
    if request.target.is_some()
        || request.expected_generation.is_some()
        || request.expected_turn_id.is_some()
        || request.request_id.is_some()
        || request.probe
        || preparation.is_some()
    {
        return Err(Error::new(
            "invalid",
            "notify/emit are targetless commands without Agent bindings",
        ));
    }
    if request.verb == "notify" {
        serde_json::from_value::<core::api::NotifyDefinitionArgs>(request.args.clone())
            .map_err(|_| Error::new("invalid", "notify definition is malformed"))?
            .validate()?;
    } else {
        let definition =
            serde_json::from_value::<core::api::EmitDefinitionArgs>(request.args.clone())
                .map_err(|_| Error::new("invalid", "emit definition is malformed"))?;
        definition.validate()?;
        if definition.topic == ATTENTION_RESERVED_TOPIC {
            return Err(Error::new(
                "invalid",
                "agent.attention is reserved for the internal attention producer",
            ));
        }
    }
    events.push(admit_command(
        tx,
        &CommandInsert::targetless_admitted(principal, request, payload_digest, now),
        json!({"command_id": request.id, "verb": request.verb}),
    )?);
    Ok(None)
}

/// Admit one internal `attention.emit`: the reserved topic only.
fn admit_attention_emit_tx(
    tx: &Transaction<'_>,
    principal: &str,
    request: &SubmitCommand,
    payload_digest: &[u8],
    preparation: &Option<WorkspacePreparationInput>,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<Option<String>> {
    if request.target.is_some()
        || request.expected_generation.is_some()
        || request.expected_turn_id.is_some()
        || request.request_id.is_some()
        || request.probe
        || preparation.is_some()
    {
        return Err(Error::new(
            "invalid",
            "attention.emit is a targetless command without Agent bindings",
        ));
    }
    let definition = serde_json::from_value::<core::api::EmitDefinitionArgs>(request.args.clone())
        .map_err(|_| Error::new("invalid", "attention.emit definition is malformed"))?;
    definition.validate()?;
    if definition.topic != ATTENTION_RESERVED_TOPIC {
        return Err(Error::new(
            "invalid",
            "attention.emit carries only the reserved attention topic",
        ));
    }
    events.push(admit_command(
        tx,
        &CommandInsert::targetless_admitted(principal, request, payload_digest, now),
        json!({"command_id": request.id, "verb": request.verb}),
    )?);
    Ok(None)
}

/// Admit one rule-derived `mail.send`/`var.set` definition.
#[allow(clippy::too_many_arguments)]
fn admit_rule_action_tx(
    tx: &Transaction<'_>,
    principal: &str,
    request: &SubmitCommand,
    payload_digest: &[u8],
    preparation: &Option<WorkspacePreparationInput>,
    now: i64,
    derived_rule: bool,
    events: &mut Vec<Event>,
) -> Result<Option<String>> {
    if !derived_rule {
        return Err(Error::new(
            "not_allowed",
            "mail.send/var.set commands must be admitted by an event-rule firing",
        ));
    }
    if request.target.is_some()
        || request.expected_generation.is_some()
        || request.expected_turn_id.is_some()
        || request.request_id.is_some()
        || request.probe
        || preparation.is_some()
    {
        return Err(Error::new(
            "invalid",
            "mail.send/var.set are targetless commands without Agent bindings",
        ));
    }
    if request.verb == "mail.send" {
        let definition =
            serde_json::from_value::<core::api::MailDefinitionArgs>(request.args.clone())
                .map_err(|_| Error::new("invalid", "mail.send definition is malformed"))?;
        definition.validate().map_err(|error| {
            Error::new(
                error.code.clone(),
                format!("mail.send definition invalid: {}", error.message),
            )
        })?;
        Store::validate_rule_mail_definition_tx(tx, principal, &definition).map_err(|error| {
            Error::new(
                error.code.clone(),
                format!("mail.send {}: {}", error.code, error.message),
            )
        })?;
    } else {
        let definition =
            serde_json::from_value::<core::api::VarDefinitionArgs>(request.args.clone())
                .map_err(|_| Error::new("invalid", "var.set definition is malformed"))?;
        definition.validate().map_err(|error| {
            Error::new(
                error.code.clone(),
                format!("var.set definition invalid: {}", error.message),
            )
        })?;
        Store::validate_rule_var_definition_tx(principal, &definition).map_err(|error| {
            Error::new(
                error.code.clone(),
                format!("var.set {}: {}", error.code, error.message),
            )
        })?;
    }
    events.push(admit_command(
        tx,
        &CommandInsert::targetless_admitted(principal, request, payload_digest, now),
        json!({"command_id": request.id, "verb": request.verb}),
    )?);
    Ok(None)
}

/// Admit one `launch`: new Agent row, creation events, and optional linked
/// first input. Returns the linked send id, if any.
#[allow(clippy::too_many_arguments)]
fn admit_launch_tx(
    tx: &Transaction<'_>,
    principal: &str,
    request: &SubmitCommand,
    payload_digest: &[u8],
    preparation: &Option<WorkspacePreparationInput>,
    now: i64,
    limits: core::api::HierarchyLimits,
    events: &mut Vec<Event>,
) -> Result<Option<String>> {
    if request.target.is_some() {
        return Err(Error::new("invalid", "launch does not take a target"));
    }
    if request.request_id.is_some() {
        return Err(Error::new(
            "invalid",
            "launch does not take a native request id",
        ));
    }
    if request.expected_turn_id.is_some()
        || request
            .expected_generation
            .is_some_and(|expected| expected != 0)
    {
        return Err(Error::new(
            "stale_generation",
            "a new launch starts from generation 0",
        ));
    }
    let parent_link = hierarchy::resolve_launch_parent(tx, principal, &request.args)?;
    if let Some(parent) = parent_link.as_ref() {
        hierarchy::check_child_fanout(tx, parent, limits)?;
    }
    let project = match parent_link.as_ref() {
        // A child inherits its parent's project label; the caller
        // value is descriptive input, not authority.
        Some(parent) => parent.project.clone(),
        None => required_string(&request.args, "project")?,
    };
    let harness = required_string(&request.args, "harness")?;
    if !matches!(
        harness.as_str(),
        "codex" | "muse" | "claude" | "opencode" | "antigravity"
    ) {
        return Err(Error::new(
            "unsupported",
            format!("unsupported launch harness {harness}"),
        ));
    }
    let profile = required_string(&request.args, "profile")?;
    let profile_snapshot = match request.args.get("profile_snapshot") {
        Some(Value::Object(_)) => request.args["profile_snapshot"].clone(),
        Some(_) => {
            return Err(Error::new(
                "invalid",
                "args.profile_snapshot must be a JSON object",
            ));
        }
        None => {
            return Err(Error::new(
                "invalid",
                format!("the {harness} harness requires a frozen profile_snapshot"),
            ));
        }
    };
    let encoded_profile_snapshot = encode_json(&profile_snapshot)?;
    let workspace = match preparation {
        Some(input) => {
            if input.project != project {
                return Err(Error::new(
                    "invalid",
                    "workspace project differs from launch",
                ));
            }
            input.worktree_path.clone()
        }
        None => required_string(&request.args, "workspace")?,
    };
    if preparation.is_some() && workspace::load_workspace_by_path(tx, &workspace)?.is_some() {
        return Err(Error::new(
            "id_conflict",
            "worktree path is already managed",
        ));
    }
    workspace::require_managed_path_ready(tx, &workspace)?;
    if request.args.get("prompt").is_some() {
        return Err(Error::new(
            "invalid",
            "use args.text for the linked launch input",
        ));
    }
    let text = optional_string(&request.args, "text")?;
    if let Some(text) = &text {
        require_nonempty(text, "args.text")?;
        check_input(text)?;
    }
    let control = hierarchy::control_from_snapshot(&profile_snapshot)?;
    let (parent_num, depth) = match parent_link.as_ref() {
        Some(parent) => (Some(parent.num), parent.depth.saturating_add(1)),
        None => (None, 0),
    };
    db!(tx.execute(
        "INSERT INTO agents(generation, project, harness, profile,
                                profile_snapshot, workspace, state,
                                parent, depth, control,
                                created_at, updated_at)
             VALUES (0, ?1, ?2, ?3, ?4, ?5, 'created', ?6, ?7, ?8, ?9, ?9)",
        params![
            project,
            harness,
            profile,
            encoded_profile_snapshot,
            workspace,
            parent_num,
            depth,
            control,
            now
        ],
    ));
    let agent_num = tx.last_insert_rowid();
    let root_num = parent_link
        .as_ref()
        .map_or(agent_num, |parent| parent.root_num);
    db!(tx.execute(
        "UPDATE agents SET root = ?1 WHERE id = ?2",
        params![root_num, agent_num],
    ));
    let agent_id = format_agent_id(agent_num);
    let agent = load_agent_by_num(tx, agent_num)?
        .ok_or_else(|| Error::new("internal", "new launch Agent disappeared"))?;
    provider_holds::require_unheld_tx(tx, &agent)?;
    let command = CommandInsert::admitted(principal, request, payload_digest, (agent_num, 0), now);
    let cause = ordinary_command_cause(command.id);
    insert_command(tx, &command, &cause)?;
    events.push(insert_daemon_event_for_command(
            tx,
            command.id,
            DaemonEventInput::new(
                now,
                "agent.created",
                "agent",
                Some(agent_num),
                Some(0),
                json!({"agent_id": agent_id, "project": project, "harness": harness, "profile": profile}),
            ),
        )?);
    events.push(insert_daemon_event_input(
        tx,
        DaemonEventInput::new(
            now,
            "command.accepted",
            "command",
            Some(agent_num),
            Some(0),
            json!({"command_id": request.id, "verb": "launch"}),
        )
        .with_cause(cause),
    )?);

    if let Some(text) = text {
        let (linked_id, event) =
            admit_linked_send(tx, principal, &request.id, agent_num, 1, &text, now)?;
        events.push(event);
        Ok(Some(linked_id))
    } else {
        Ok(None)
    }
}

/// Admit one `send`/`stop` to its exact target: generation and turn
/// binding first, then the shared member gates (lane, FIFO, notices).
fn admit_send_stop_tx(
    tx: &Transaction<'_>,
    principal: &str,
    request: &SubmitCommand,
    payload_digest: &[u8],
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    if request.request_id.is_some() {
        return Err(Error::new(
            "invalid",
            "send and stop do not take a native request id",
        ));
    }
    let target = request
        .target
        .as_deref()
        .ok_or_else(|| Error::new("invalid", "send and stop require an Agent target"))?;
    let agent_num = parse_agent_id(target)?;
    let agent = load_agent_by_num(tx, agent_num)?
        .ok_or_else(|| Error::new("not_found", format!("Agent {target} was not found")))?;
    if let Some(expected) = request.expected_generation
        && expected != agent.generation
    {
        return Err(Error::new(
            "stale_generation",
            format!(
                "expected generation {expected}, current is {}",
                agent.generation
            ),
        ));
    }
    if let Some(expected_turn) = request.expected_turn_id.as_deref()
        && agent.active_turn.as_deref() != Some(expected_turn)
    {
        return Err(Error::new(
            "stale_generation",
            "expected turn does not match the current Agent turn",
        ));
    }
    if request.verb == "send" {
        admit_send_member(tx, principal, request, payload_digest, &agent, now, events)?;
    } else {
        admit_stop_member(tx, principal, request, payload_digest, &agent, now, events)?;
    }
    Ok(())
}

/// Admit one `worktree.remove`: binding and removability checks, then the
/// frozen removal args.
fn admit_worktree_remove_tx(
    tx: &Transaction<'_>,
    principal: &str,
    request: &SubmitCommand,
    payload_digest: &[u8],
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    let target = request
        .target
        .as_deref()
        .ok_or_else(|| Error::new("invalid", "worktree.remove requires an Agent"))?;
    let agent_num = parse_agent_id(target)?;
    let agent = load_agent_by_num(tx, agent_num)?
        .ok_or_else(|| Error::new("not_found", "workspace owner was not found"))?;
    if request.request_id.is_some()
        || request.expected_turn_id.is_some()
        || request
            .expected_generation
            .is_some_and(|expected| expected != agent.generation)
        || request.args.as_object().is_none_or(|args| !args.is_empty())
    {
        return Err(Error::new(
            "invalid",
            "invalid worktree.remove binding or arguments",
        ));
    }
    let workspace = workspace::load_workspace(tx, agent_num)?
        .ok_or_else(|| Error::new("not_found", "Agent has no managed worktree"))?;
    workspace::require_workspace_removable(tx, &agent, &workspace)?;
    let args = json!({"target_generation": workspace.target_generation});
    events.push(admit_command(
        tx,
        &CommandInsert {
            args: &args,
            ..CommandInsert::admitted(
                principal,
                request,
                payload_digest,
                (agent_num, agent.generation),
                now,
            )
        },
        json!({"command_id": request.id, "verb": "worktree.remove"}),
    )?);
    Ok(())
}

/// Admit one `resume`: terminal-state and native-subject checks, then the
/// resumed generation with optional linked input. Returns the linked id.
fn admit_resume_tx(
    tx: &Transaction<'_>,
    principal: &str,
    request: &SubmitCommand,
    payload_digest: &[u8],
    now: i64,
    events: &mut Vec<Event>,
) -> Result<Option<String>> {
    let target = request
        .target
        .as_deref()
        .ok_or_else(|| Error::new("invalid", "resume requires an Agent target"))?;
    let agent_num = parse_agent_id(target)?;
    let agent = load_agent_by_num(tx, agent_num)?
        .ok_or_else(|| Error::new("not_found", format!("Agent {target} was not found")))?;
    if let Some(path) = &agent.workspace {
        workspace::require_managed_path_ready(tx, path)?;
    }
    if !matches!(
        agent.harness.as_str(),
        "codex" | "muse" | "claude" | "opencode" | "antigravity"
    ) {
        return Err(Error::new(
            "unsupported",
            "resume is only supported for Codex, Muse, Claude, OpenCode, or Antigravity Agents with a retained native subject",
        ));
    }
    if let Some(expected) = request.expected_generation
        && expected != agent.generation
    {
        return Err(Error::new(
            "stale_generation",
            format!(
                "expected generation {expected}, current is {}",
                agent.generation
            ),
        ));
    }
    if request.expected_turn_id.is_some() {
        return Err(Error::new(
            "stale_generation",
            "resume cannot target an active turn",
        ));
    }
    if request.request_id.is_some() {
        return Err(Error::new(
            "invalid",
            "resume does not take a native request id",
        ));
    }
    if !matches!(
        agent.state,
        AgentState::Stopped | AgentState::Failed | AgentState::Lost
    ) || agent.active_turn.is_some()
    {
        return Err(Error::new(
            "bad_state",
            "resume requires a stopped, failed, or lost Agent with no active turn",
        ));
    }
    if has_pending_resume_tx(tx, agent_num, agent.generation, &request.id)? {
        return Err(Error::new(
            "bad_state",
            "a pending resume already targets this Agent generation",
        ));
    }
    if has_in_flight_stop_tx(tx, agent_num, agent.generation)? {
        return Err(Error::new(
            "bad_state",
            "a dispatched or unknown Stop still targets this Agent generation",
        ));
    }
    if [&agent.native_scope, &agent.native_ref]
        .iter()
        .any(|value| value.as_deref().is_none_or(|value| value.trim().is_empty()))
    {
        return Err(Error::new(
            "unsupported",
            "resume requires a retained native_scope and native_ref",
        ));
    }
    if request
        .args
        .as_object()
        .is_some_and(|args| args.keys().any(|key| key != "text"))
    {
        return Err(Error::new(
            "unsupported",
            "resume accepts only new linked input text; profile and subject are retained",
        ));
    }
    let text = optional_string(&request.args, "text")?;
    if let Some(text) = &text {
        require_nonempty(text, "args.text")?;
        check_input(text)?;
        provider_holds::require_unheld_tx(tx, &agent)?;
    }
    projects::validate_project_resume_action_tx(
        tx,
        principal,
        &agent.id,
        agent.generation,
        request.expected_generation,
        text.is_some(),
    )?;
    let next_generation = agent
        .generation
        .checked_add(1)
        .ok_or_else(|| Error::new("bad_state", "Agent generation cannot be incremented"))?;
    let resume_args = json!({});
    events.push(admit_command(
        tx,
        &CommandInsert {
            args: &resume_args,
            ..CommandInsert::admitted(
                principal,
                request,
                payload_digest,
                (agent_num, agent.generation),
                now,
            )
        },
        json!({"command_id": request.id, "verb": "resume"}),
    )?);

    if let Some(text) = text {
        let (linked_id, event) = admit_linked_send(
            tx,
            principal,
            &request.id,
            agent_num,
            next_generation,
            &text,
            now,
        )?;
        events.push(event);
        Ok(Some(linked_id))
    } else {
        Ok(None)
    }
}

/// Admit one `configure`: Owner authority, live-generation check, and
/// field validation. The overlay itself is stored by the Router later.
fn admit_configure_tx(
    tx: &Transaction<'_>,
    principal: &str,
    request: &SubmitCommand,
    payload_digest: &[u8],
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    if request.selector.is_some()
        || request.expected_turn_id.is_some()
        || request.request_id.is_some()
        || request.probe
    {
        return Err(Error::new(
            "invalid",
            "configure takes only a target and an optional generation binding",
        ));
    }
    let target = request
        .target
        .as_deref()
        .ok_or_else(|| Error::new("invalid", "configure requires an Agent target"))?;
    let agent_num = parse_agent_id(target)?;
    let agent = load_agent_by_num(tx, agent_num)?
        .ok_or_else(|| Error::new("not_found", format!("Agent {target} was not found")))?;
    if let Some(expected) = request.expected_generation
        && expected != agent.generation
    {
        return Err(Error::new(
            "stale_generation",
            format!(
                "expected generation {expected}, current is {}",
                agent.generation
            ),
        ));
    }
    if principal != "owner" {
        return Err(Error::new(
            "not_allowed",
            "configure requires the Owner principal",
        ));
    }
    if agent.generation < 1
        || agent.is_terminal()
        || !matches!(
            agent.state,
            AgentState::Idle | AgentState::Busy | AgentState::Waiting
        )
    {
        return Err(Error::new(
            "bad_state",
            "configure requires a live Agent generation with a native session",
        ));
    }
    // Validate the requested fields and harness support now; the
    // overlay itself is stored by the Router after dispatch, which
    // settles the command applied only once the values are stored.
    let _ = resolve_configure(&request.args, &agent.harness, &agent.profile_snapshot)?;
    events.push(admit_command(
        tx,
        &CommandInsert::admitted(
            principal,
            request,
            payload_digest,
            (agent_num, agent.generation),
            now,
        ),
        json!({"command_id": request.id, "verb": "configure"}),
    )?);
    Ok(())
}

/// Admit one turn-control command (`steer`, `interrupt`, `approve`, `deny`,
/// `answer`): generation, state, turn, and pending-request binding.
fn admit_control_tx(
    tx: &Transaction<'_>,
    principal: &str,
    request: &SubmitCommand,
    payload_digest: &[u8],
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    let target = request
        .target
        .as_deref()
        .ok_or_else(|| Error::new("invalid", "control command requires an Agent target"))?;
    let agent_num = parse_agent_id(target)?;
    let agent = load_agent_by_num(tx, agent_num)?
        .ok_or_else(|| Error::new("not_found", format!("Agent {target} was not found")))?;
    // Control-verb support is owned by the PluginHost manifest check;
    // Store admits every known control verb and retains the live
    // generation, state, turn, and request checks below.
    if let Some(expected) = request.expected_generation
        && expected != agent.generation
    {
        return Err(Error::new(
            "stale_generation",
            format!(
                "expected generation {expected}, current is {}",
                agent.generation
            ),
        ));
    }
    if !matches!(agent.state, AgentState::Busy | AgentState::Waiting) {
        return Err(Error::new(
            "bad_state",
            "turn controls require a busy or waiting Agent",
        ));
    }
    let turn_id = agent
        .active_turn
        .clone()
        .ok_or_else(|| Error::new("bad_state", "turn control requires an exact active turn"))?;
    if request
        .expected_turn_id
        .as_deref()
        .is_some_and(|expected| expected != turn_id.as_str())
    {
        return Err(Error::new(
            "stale_generation",
            "expected turn does not match the current Agent turn",
        ));
    }

    let request_id = match request.verb.as_str() {
        "steer" | "interrupt" => {
            if request.request_id.is_some() {
                return Err(Error::new(
                    "invalid",
                    "steer and interrupt do not take a native request id",
                ));
            }
            None
        }
        _ => {
            let pending = if let Some(request_id) = request.request_id.as_deref() {
                require_nonempty(request_id, "request_id")?;
                let (pending, state) =
                    load_pending_request(tx, agent_num, agent.generation, request_id)?.ok_or_else(
                        || {
                            Error::new(
                                "not_found",
                                "pending request was not found in this Agent generation",
                            )
                        },
                    )?;
                if state != "pending" {
                    return Err(Error::new(
                        "source_integrity",
                        "native request is no longer pending",
                    ));
                }
                pending
            } else {
                capture_single_pending_request(tx, agent_num, agent.generation)?
            };
            if pending.turn_id.as_str() != turn_id.as_str()
                || request
                    .expected_turn_id
                    .as_deref()
                    .is_some_and(|expected| expected != pending.turn_id.as_str())
            {
                return Err(Error::new(
                    "stale_generation",
                    "pending request does not belong to the captured active turn",
                ));
            }
            if !agent.pending.contains(&pending) {
                return Err(Error::new(
                    "source_integrity",
                    "pending request row does not match the Agent projection",
                ));
            }
            if request.verb == "answer" && contains_secret_marker(&pending.details) {
                return Err(Error::new(
                    "secret_in_payload",
                    "answer to a secret question is not permitted",
                ));
            }
            if has_open_request_reply(tx, agent_num, agent.generation, &pending.id, None)? {
                return Err(Error::new(
                    "bad_state",
                    "a reply command is already unresolved for this native request",
                ));
            }
            Some(pending.id)
        }
    };
    if request.verb == "steer" {
        let text = required_string(&request.args, "text")?;
        check_input(&text)?;
        provider_holds::require_unheld_tx(tx, &agent)?;
    }
    if request.verb == "answer" {
        eliot_core::requests::AnswerArgs::parse(&request.args)?;
    }
    if matches!(request.verb.as_str(), "approve" | "deny") {
        eliot_core::requests::ApprovalArgs::parse(&request.args)?;
    }
    let mut params = json!({
        "agent_id": agent.id,
        "generation": agent.generation,
        "command_id": request.id,
        "verb": request.verb,
        "turn_id": turn_id,
        "args": request.args,
    });
    if let Some(request_id) = request_id.as_deref() {
        params["request_id"] = Value::String(request_id.to_owned());
    }
    check_rpc_frame("agent.command", &params)?;
    events.push(admit_command(
        tx,
        &CommandInsert {
            turn_id: Some(&turn_id),
            request_id: request_id.as_deref(),
            ..CommandInsert::admitted(
                principal,
                request,
                payload_digest,
                (agent_num, agent.generation),
                now,
            )
        },
        json!({
            "command_id": request.id,
            "verb": request.verb,
            "turn_id": turn_id,
            "request_id": request_id
        }),
    )?);
    Ok(())
}

/// Start-command postlude shared by `launch` and `resume`: frozen
/// workspace preparation, reload of the admitted command, and the
/// capacity reservation for the next generation.
fn admit_start_postlude_tx(
    tx: &Transaction<'_>,
    request: &SubmitCommand,
    preparation: &Option<WorkspacePreparationInput>,
    linked: &Option<String>,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<(Command, Option<Command>)> {
    if matches!(request.verb.as_str(), "launch" | "resume") {
        workspace::admit_workspace_preparation(tx, request, preparation.as_ref(), now, events)?;
    }
    let command = load_command(tx, &request.id)?
        .ok_or_else(|| Error::new("internal", "new command was not visible in its transaction"))?;
    let linked_command = match linked.as_deref() {
        Some(linked_id) => load_command(tx, linked_id)?,
        None => None,
    };
    if matches!(request.verb.as_str(), "launch" | "resume") {
        let agent_num = command
            .target
            .as_deref()
            .map(parse_agent_id)
            .transpose()?
            .ok_or_else(|| Error::new("internal", "start admission has no Agent"))?;
        let agent = load_agent_by_num(tx, agent_num)?
            .ok_or_else(|| Error::new("internal", "start admission Agent disappeared"))?;
        let generation = if request.verb == "launch" {
            1
        } else {
            agent
                .generation
                .checked_add(1)
                .ok_or_else(|| Error::new("bad_state", "generation overflow"))?
        };
        capacity::reserve_tx(tx, &agent, generation)?;
    }
    Ok((command, linked_command))
}

pub(super) fn submit_tx(
    tx: &Transaction<'_>,
    principal: &str,
    request: SubmitCommand,
    preparation: Option<WorkspacePreparationInput>,
    now: i64,
    derived_rule: bool,
    limits: core::api::HierarchyLimits,
) -> Result<Admission> {
    let (payload_digest, replay) = submit_head_tx(tx, principal, &request, now, derived_rule)?;
    if let Some(admission) = replay {
        return Ok(admission);
    }
    if request.selector.is_some() {
        return submit_selector_tx(
            tx,
            principal,
            request,
            &payload_digest,
            now,
            derived_rule,
            limits,
        );
    }
    submit_target_guards_tx(tx, &request, &preparation)?;

    let mut events = Vec::new();
    let linked = match request.verb.as_str() {
        "attach" | "detach" => {
            observer::admit(tx, principal, &request, &payload_digest, now, &mut events)?;
            None
        }
        "run" => admit_run_tx(
            tx,
            principal,
            &request,
            &payload_digest,
            &preparation,
            now,
            &mut events,
        )?,
        "git.fetch" => admit_git_fetch_tx(
            tx,
            principal,
            &request,
            &payload_digest,
            &preparation,
            now,
            &mut events,
        )?,
        "notify" | "emit" => admit_notify_emit_tx(
            tx,
            principal,
            &request,
            &payload_digest,
            &preparation,
            now,
            &mut events,
        )?,
        "attention.emit" => admit_attention_emit_tx(
            tx,
            principal,
            &request,
            &payload_digest,
            &preparation,
            now,
            &mut events,
        )?,
        "mail.send" | "var.set" => admit_rule_action_tx(
            tx,
            principal,
            &request,
            &payload_digest,
            &preparation,
            now,
            derived_rule,
            &mut events,
        )?,
        "inject" => {
            events.push(context::admit_tx(
                tx,
                principal,
                &request,
                &payload_digest,
                now,
            )?);
            None
        }
        "launch" => admit_launch_tx(
            tx,
            principal,
            &request,
            &payload_digest,
            &preparation,
            now,
            limits,
            &mut events,
        )?,
        "send" | "stop" => {
            admit_send_stop_tx(tx, principal, &request, &payload_digest, now, &mut events)?;
            None
        }
        "worktree.remove" => {
            admit_worktree_remove_tx(tx, principal, &request, &payload_digest, now, &mut events)?;
            None
        }
        "resume" => admit_resume_tx(tx, principal, &request, &payload_digest, now, &mut events)?,
        "configure" => {
            admit_configure_tx(tx, principal, &request, &payload_digest, now, &mut events)?;
            None
        }
        "steer" | "interrupt" | "approve" | "deny" | "answer" => {
            admit_control_tx(tx, principal, &request, &payload_digest, now, &mut events)?;
            None
        }
        verb => {
            return Err(Error::new(
                "unsupported",
                format!("Store does not admit the {verb} verb"),
            ));
        }
    };

    let (command, linked_command) =
        admit_start_postlude_tx(tx, &request, &preparation, &linked, now, &mut events)?;
    Ok(Admission {
        command,
        linked_command,
        events,
        is_new: true,
    })
}

/// Whether a reply command is still unresolved for this native request.
/// Unknown and inflight always fence. A definite Applied only stops fencing
/// once the persisted current offer is genuinely fresh past every Applied
/// decision: the harness normalizes its current offer binding as an integer
/// `offer_seq` in pending details and records the decided binding as integer
/// `decided_offer_seq` in each Applied reply outcome. All Applied decided
/// strictly below current are consumed; anything missing, malformed, equal,
/// or older stays blocking. Harnesses without multi-stage offers omit both
/// fields and keep the single-reply fence.
pub(super) fn request_reply_blocked(
    connection: &Connection,
    agent: i64,
    generation: i64,
    request_id: &str,
    except_command: Option<&str>,
) -> Result<bool> {
    let mut stmt = connection
        .prepare(
            "SELECT state, outcome FROM commands
             WHERE target = ?1 AND generation = ?2 AND request_id = ?3
               AND state IN ('accepted', 'queued', 'dispatched', 'unknown', 'applied')
               AND (?4 IS NULL OR id != ?4)",
        )
        .map_err(sql_error)?;
    let rows: Vec<(String, Option<String>)> = stmt
        .query_map(
            params![agent, generation, request_id, except_command],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(sql_error)?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(sql_error)?;
    if rows.iter().any(|(state, _)| {
        matches!(
            state.as_str(),
            "accepted" | "queued" | "dispatched" | "unknown"
        )
    }) {
        return Ok(true);
    }
    let applied: Vec<&Option<String>> = rows
        .iter()
        .filter(|(state, _)| state == "applied")
        .map(|(_, outcome)| outcome)
        .collect();
    if applied.is_empty() {
        return Ok(false);
    }
    let Some((pending, state)) = load_pending_request(connection, agent, generation, request_id)?
    else {
        return Ok(true);
    };
    if state != "pending" {
        return Ok(true);
    }
    let Some(current) = pending.details.get("offer_seq").and_then(Value::as_i64) else {
        return Ok(true);
    };
    for outcome in applied {
        let Some(text) = outcome.as_deref() else {
            return Ok(true);
        };
        let Ok(value) = serde_json::from_str::<Value>(text) else {
            return Ok(true);
        };
        let Some(decided) = value.get("decided_offer_seq").and_then(Value::as_i64) else {
            return Ok(true);
        };
        if decided >= current {
            return Ok(true);
        }
    }
    Ok(false)
}
