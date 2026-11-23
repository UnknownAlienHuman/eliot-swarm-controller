//! Switch orchestration uses only existing command rows and normal launch/handoff admission.
use super::*;
use eliot_core::api::{
    HandoffReason, ORGANIZATION_CHANGED_KIND, OrgRowChange, OrganizationChangedEvent,
    ReparentRequest, SwitchAgent, SwitchLaunch, SwitchRequest, SwitchResponse,
};

mod automatic;
mod recovery;
pub use automatic::{AutomaticHandoff, HANDOFF_QUEUED, HANDOFF_SETTLED, HANDOFF_WAITING};
pub(super) use recovery::{has_releasable_switch_fence, owner_release_switch_fence_tx};

const METHOD: &str = "swarm.switch";
const PROVIDER_FAILURE: &str = "provider_failure";
const PROVIDER_AUTHORITY: &str = "provider_authority";
const SESSION_GROWTH_AUTHORITY: &str = "session_growth_authority";
const OWNER_AUTOMATION: &str = "owner_automation";
const CONTINUATION: &str = "continuation";
const REQUESTED_BY: &str = "requested_by";

pub type SwitchLiveCheckout = (String, i64, String, String);

pub struct SwitchReservation<'a> {
    pub owner_process: Option<&'a OwnerProcess>,
    pub profile_authority: Option<[u8; 32]>,
    pub source_checkout: Option<(&'a str, &'a str)>,
    pub live_checkouts: &'a [SwitchLiveCheckout],
}

pub struct SwitchLaunchCheckout<'a> {
    pub root: &'a str,
    pub key: &'a str,
    pub live_checkouts: &'a [SwitchLiveCheckout],
    pub source_clean: Option<bool>,
}

/// A resumed source must not race the successor after the switch has stopped it.
pub(super) fn require_no_active_checkout_switch(connection: &Connection, agent: i64) -> Result<()> {
    let active: bool = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM switch_checkout_owners
         WHERE agent = ?1 AND switch_command IS NOT NULL)",
            [agent],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if active {
        return Err(Error::new(
            "busy",
            "checkout is fenced by an unsettled switch",
        ));
    }
    Ok(())
}

/// Refuse a new start while another live Agent owns this checkout. `None` is a
/// new launch; a resume supplies its existing Agent id.
pub(super) fn require_checkout_owner_tx(
    connection: &Connection,
    workspace: Option<&str>,
    agent: Option<i64>,
) -> Result<()> {
    let Some(workspace) = workspace else {
        return Ok(());
    };
    let workspace_key = core::workspace_path_key(workspace);
    let mut statement = connection
        .prepare_cached(
            "SELECT checkout_key, agent FROM switch_checkout_owners
             WHERE switch_command IS NULL",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })
        .map_err(sql_error)?;
    for row in rows {
        let (checkout_key, owner) = row.map_err(sql_error)?;
        if Some(owner) == agent || !checkout_contains_workspace(&checkout_key, &workspace_key) {
            continue;
        }
        let Some(owner_agent) = load_agent_by_num(connection, owner)? else {
            continue;
        };
        let owner_is_live = !owner_agent.is_terminal()
            || handoff::session_exit_blocker(connection, &owner_agent)?.is_some();
        if owner_is_live {
            return Err(Error::new(
                "bad_state",
                format!(
                    "canonical checkout is owned by Agent {}#{}",
                    owner_agent.id, owner_agent.generation
                ),
            ));
        }
    }
    Ok(())
}

fn checkout_contains_workspace(checkout_key: &str, workspace_key: &str) -> bool {
    if checkout_key == workspace_key {
        return true;
    }
    let checkout_root = checkout_key.trim_end_matches('/');
    if checkout_root.is_empty() {
        return workspace_key.starts_with('/');
    }
    workspace_key
        .strip_prefix(checkout_root)
        .is_some_and(|suffix| suffix.starts_with('/'))
}

pub(super) fn require_checkout_launch_tx(
    tx: &Transaction<'_>,
    id: &str,
    agent: i64,
    workspace: Option<&str>,
) -> Result<()> {
    let switch_parent: Option<(Option<String>, Option<String>)> = tx
        .query_row(
            "SELECT c.parent_command, p.state FROM commands c
             LEFT JOIN commands p ON p.id = c.parent_command
             WHERE c.id = ?1 AND c.link_kind = 'switch' AND c.ordinal = 0",
            [id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(sql_error)?;
    let parent = match switch_parent {
        Some((Some(parent), Some(state))) if state == "dispatched" => Some(parent),
        Some(_) => {
            return Err(Error::new(
                "bad_state",
                "switch launch requires its dispatched parent",
            ));
        }
        None => None,
    };
    let Some(workspace) = workspace else {
        return Ok(());
    };
    let key = core::workspace_path_key(workspace);
    let mut statement = tx
        .prepare_cached(
            "SELECT checkout_key, switch_command FROM switch_checkout_owners
         WHERE switch_command IS NOT NULL",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(sql_error)?;
    for row in rows {
        let (checkout, switch) = row.map_err(sql_error)?;
        if parent.as_deref() != Some(&switch) && checkout_contains_workspace(&checkout, &key) {
            return Err(Error::new(
                "busy",
                "checkout is fenced by an unsettled switch",
            ));
        }
    }
    if parent.is_none() {
        require_checkout_owner_tx(tx, Some(workspace), Some(agent))?;
    }
    Ok(())
}
const SOURCE_STOP_COMMAND: &str = "switch_source_stop_command";
const SOURCE_CHECKOUT: &str = "switch_source_checkout";
const SOURCE_CHECKOUT_ROOT: &str = "switch_source_checkout_root";
const SOURCE_CHECKOUT_KEY: &str = "switch_source_checkout_key";
const TARGET_CHECKOUT_ROOT: &str = "switch_target_checkout_root";
const TARGET_CHECKOUT_KEY: &str = "switch_target_checkout_key";

/// Retained reason of a daemon-initiated continuation.
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
pub struct SwitchContinuation {
    pub reason: HandoffReason,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<core::ProviderRestrictionKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub held_since: Option<i64>,
}

#[derive(Clone, Copy)]
pub struct ProviderSwitchFailure {
    pub kind: core::ProviderRestrictionKind,
    pub authority: Option<[u8; 32]>,
}

impl ProviderSwitchFailure {
    pub fn require_authority(self, snapshot: &Value) -> Result<[u8; 32]> {
        let candidate = core::provider_switch_authority(snapshot)?;
        if Some(candidate) == self.authority {
            return Ok(candidate);
        }
        Err(Error::new(
            "not_allowed",
            "automatic switch authority differs from the original launch",
        ))
    }
}

pub const PROVIDER_SWITCH_QUEUED: &str = "provider.failover_queued";
pub const PROVIDER_SWITCH_SETTLED: &str = "provider.failover_settled";

pub struct SwitchWork {
    pub request: SwitchRequest,
    pub plan: SwitchResponse,
    pub source_stop_command: Option<String>,
    pub source_checkout: Option<String>,
    pub source_checkout_root: Option<String>,
    pub source_checkout_key: Option<String>,
    pub provider_failure: Option<ProviderSwitchFailure>,
    pub profile_authority: Option<[u8; 32]>,
    pub owner_automation: bool,
    pub continuation: Option<SwitchContinuation>,
    /// The manager principal that requested this switch.
    pub requested_by: Option<String>,
}

struct RetainedSwitch {
    request: SwitchRequest,
    source_stop_command: Option<String>,
    source_checkout: Option<String>,
    source_checkout_root: Option<String>,
    source_checkout_key: Option<String>,
    target_checkout_root: Option<String>,
    target_checkout_key: Option<String>,
    provider_failure: Option<ProviderSwitchFailure>,
    profile_authority: Option<[u8; 32]>,
    owner_automation: bool,
    continuation: Option<SwitchContinuation>,
    requested_by: Option<String>,
}

fn decode_switch(mut args: Value) -> Result<RetainedSwitch> {
    let malformed = || Error::new("source_integrity", "switch request malformed");
    let args_map = args.as_object_mut().ok_or_else(malformed)?;
    let failure = args_map
        .remove(PROVIDER_FAILURE)
        .map(serde_json::from_value)
        .transpose()
        .map_err(|_| malformed())?;
    let authority = args_map
        .remove(PROVIDER_AUTHORITY)
        .map(serde_json::from_value)
        .transpose()
        .map_err(|_| malformed())?;
    let profile_authority = args_map
        .remove(SESSION_GROWTH_AUTHORITY)
        .map(serde_json::from_value)
        .transpose()
        .map_err(|_| malformed())?;
    let owner_automation = args_map
        .remove(OWNER_AUTOMATION)
        .map(serde_json::from_value)
        .transpose()
        .map_err(|_| malformed())?
        .unwrap_or(false);
    let continuation = args_map
        .remove(CONTINUATION)
        .map(serde_json::from_value)
        .transpose()
        .map_err(|_| malformed())?;
    let requested_by = args_map
        .remove(REQUESTED_BY)
        .map(serde_json::from_value)
        .transpose()
        .map_err(|_| malformed())?;
    let source_stop_command = args_map
        .remove(SOURCE_STOP_COMMAND)
        .map(serde_json::from_value)
        .transpose()
        .map_err(|_| malformed())?;
    let source_checkout = args_map
        .remove(SOURCE_CHECKOUT)
        .map(serde_json::from_value)
        .transpose()
        .map_err(|_| malformed())?;
    let source_checkout_root = args_map
        .remove(SOURCE_CHECKOUT_ROOT)
        .map(serde_json::from_value)
        .transpose()
        .map_err(|_| malformed())?;
    let source_checkout_key = args_map
        .remove(SOURCE_CHECKOUT_KEY)
        .map(serde_json::from_value)
        .transpose()
        .map_err(|_| malformed())?;
    let target_checkout_root = args_map
        .remove(TARGET_CHECKOUT_ROOT)
        .map(serde_json::from_value)
        .transpose()
        .map_err(|_| malformed())?;
    let target_checkout_key = args_map
        .remove(TARGET_CHECKOUT_KEY)
        .map(serde_json::from_value)
        .transpose()
        .map_err(|_| malformed())?;
    let request = serde_json::from_value(args).map_err(|_| malformed())?;
    Ok(RetainedSwitch {
        request,
        source_stop_command,
        source_checkout,
        source_checkout_root,
        source_checkout_key,
        target_checkout_root,
        target_checkout_key,
        provider_failure: failure.map(|kind| ProviderSwitchFailure { kind, authority }),
        profile_authority,
        owner_automation,
        continuation,
        requested_by,
    })
}

/// The retained arguments and replay digest. A manager's request binds its
/// principal, so another caller cannot read it back by reusing the ID.
fn switch_args(principal: &str, request: &SwitchRequest) -> Result<(Value, Vec<u8>)> {
    let mut args = serde_json::to_value(request)
        .map_err(|_| Error::new("internal", "cannot encode switch"))?;
    if principal != "owner" {
        args[REQUESTED_BY] = json!(principal);
    }
    let digest = canonical::digest(&args)?;
    Ok((args, digest))
}

fn require_switch_principal(principal: &str) -> Result<()> {
    if principal == "owner" || principal.starts_with("agent:") {
        Ok(())
    } else {
        Err(Error::new(
            "not_allowed",
            "only the Owner or a manager may switch Agents",
        ))
    }
}

/// A manager moves a worker in its own controlled scope to a profile or role,
/// in the worker's own worktree.
fn require_switch_caller(
    tx: &Transaction<'_>,
    principal: &str,
    request: &SwitchRequest,
) -> Result<()> {
    if principal == "owner" {
        return Ok(());
    }
    let caller = hierarchy::parse_caller(tx, principal)?;
    let hierarchy::Caller::Agent { num, id, .. } = &caller else {
        return Err(Error::new("not_allowed", "switch caller is not a manager"));
    };
    let manager = load_agent_by_num(tx, *num)?
        .ok_or_else(|| Error::new("stale_generation", "caller generation is not current"))?;
    observer::require_controlled(&manager)?;
    let launch = &request.launch;
    if request.from == *id
        || launch.harness.is_some()
        || launch.project.is_some()
        || launch.workspace.is_some()
        || launch.worktree.is_some()
        || launch.base.is_some()
    {
        return Err(Error::new(
            "not_allowed",
            "a manager switches another worker to a profile or role in that worker's worktree",
        ));
    }
    let source = load_agent_by_num(tx, parse_agent_id(&request.from)?)?
        .ok_or_else(|| Error::new("not_found", "source Agent missing"))?;
    hierarchy::caller_may_command(tx, &caller, &source)
}

/// The exact check for a successor nobody with Owner authority chose by hand.
fn require_handoff_bounds(
    tx: &Connection,
    source: &Agent,
    successor: &Value,
    workspace: Option<&str>,
    profile: Option<&str>,
    requested_by: Option<&str>,
) -> Result<()> {
    if let Some(reason) = core::handoff_authority_refusal(&source.profile_snapshot, successor) {
        return Err(Error::new(
            "not_allowed",
            format!("successor authority refused: {reason}"),
        ));
    }
    if workspace != source.workspace.as_deref() {
        return Err(Error::new(
            "not_allowed",
            "a bounded handoff must keep the source worktree",
        ));
    }
    if let Some(principal) = requested_by {
        let caller = hierarchy::parse_caller(tx, principal)?;
        hierarchy::caller_may_command(tx, &caller, source)?;
        let hierarchy::Caller::Agent { num, .. } = caller else {
            return Err(Error::new("not_allowed", "switch caller is not a manager"));
        };
        let manager = load_agent_by_num(tx, num)?
            .ok_or_else(|| Error::new("stale_generation", "caller generation is not current"))?;
        let allowed = profile == Some(source.profile.as_str())
            || manager.profile_snapshot["child_profiles"]
                .as_array()
                .is_some_and(|names| names.iter().any(|name| name.as_str() == profile));
        if !allowed {
            return Err(Error::new(
                "not_allowed",
                "successor profile is outside the manager's child profiles",
            ));
        }
    }
    Ok(())
}

/// One canonical checkout has one durable owner and at most one in-flight
/// switch. Both facts survive event retention and explicit path overrides.
fn switch_blocker(
    tx: &Connection,
    request: &SwitchRequest,
    checkout_key: Option<&str>,
) -> Result<Option<Error>> {
    let open: bool = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM commands INDEXED BY commands_unsettled_created
             WHERE state IN ('accepted', 'queued', 'dispatched', 'unknown')
               AND verb = 'switch' AND method = ?1 AND id <> ?2
               AND json_extract(args, '$.from') = ?3
               AND json_extract(args, '$.from_generation') = ?4)",
            params![METHOD, request.id, request.from, request.from_generation],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if open {
        return Ok(Some(Error::new(
            "busy",
            format!("Agent {} already has a switch in progress", request.from),
        )));
    }
    let source = load_agent_by_num(tx, parse_agent_id(&request.from)?)?
        .ok_or_else(|| Error::new("not_found", "source Agent missing"))?;
    if source.generation != request.from_generation {
        return Err(Error::new("stale_generation", "source generation moved"));
    }
    let source_num = parse_agent_id(&source.id)?;
    let successor: Option<i64> = tx
        .query_row(
            "SELECT successor FROM agent_successors WHERE agent = ?1 AND generation = ?2",
            params![source_num, source.generation],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)?;
    if let Some(successor) = successor {
        return Ok(Some(Error::new(
            "bad_state",
            format!(
                "Agent {} already handed this checkout to {}; switch that Agent instead",
                source.id,
                format_agent_id(successor)
            ),
        )));
    }
    if let Some(checkout_key) = checkout_key
        && let Some((owner, generation, active_switch)) = checkout_fence(tx, checkout_key)?
        && (owner != source_num || active_switch.is_some())
    {
        return Ok(Some(Error::new(
            "bad_state",
            format!(
                "canonical checkout is owned by {}#{}{}",
                format_agent_id(owner),
                generation,
                active_switch
                    .map(|id| format!(" under switch {id}"))
                    .unwrap_or_default()
            ),
        )));
    }
    Ok(None)
}

fn checkout_fence(
    connection: &Connection,
    checkout_key: &str,
) -> Result<Option<(i64, i64, Option<String>)>> {
    connection
        .query_row(
            "SELECT agent, generation, switch_command FROM switch_checkout_owners
             WHERE checkout_key = ?1",
            [checkout_key],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(sql_error)
}

fn live_workspace_inputs(connection: &Connection) -> Result<Vec<(String, i64, String)>> {
    let mut statement = connection
        .prepare_cached(
            "SELECT a.id, a.generation, a.workspace FROM agents a
             WHERE a.observe_only = 0 AND a.workspace IS NOT NULL
               AND EXISTS (
                   SELECT 1 FROM processes p
                   WHERE p.owner = 'agent:a' || a.id || '#' || a.generation
                     AND p.state IN ('running','unknown')
               ) ORDER BY a.id, a.generation",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                format_agent_id(row.get::<_, i64>(0)?),
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .map_err(sql_error)?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .map_err(sql_error)
}

fn require_live_checkout_snapshot(
    tx: &Connection,
    snapshot: &[(String, i64, String, String)],
    checkout_key: &str,
    except_agent: &str,
) -> Result<()> {
    let current = live_workspace_inputs(tx)?;
    if current.len() != snapshot.len()
        || current.iter().any(|(agent, generation, workspace)| {
            !snapshot
                .iter()
                .any(|(known_agent, known_generation, known_workspace, _)| {
                    known_agent == agent
                        && known_generation == generation
                        && known_workspace == workspace
                })
        })
    {
        return Err(Error::new(
            "busy",
            "live workspace custody changed during switch preflight; retry the switch",
        ));
    }
    for (agent, generation, workspace) in current {
        let key = snapshot
            .iter()
            .find(|(known_agent, known_generation, known_workspace, _)| {
                known_agent == &agent
                    && *known_generation == generation
                    && known_workspace == &workspace
            })
            .map(|(_, _, _, key)| key.as_str())
            .ok_or_else(|| Error::new("source_integrity", "live checkout identity is missing"))?;
        if key == checkout_key && agent != except_agent {
            return Err(Error::new(
                "busy",
                format!("canonical checkout has a competing live writer {agent}#{generation}"),
            ));
        }
    }
    Ok(())
}

fn claim_source_checkout(
    tx: &Transaction<'_>,
    checkout_key: &str,
    checkout_root: &str,
    source: &Agent,
    switch_id: &str,
    now: i64,
) -> Result<()> {
    let agent = parse_agent_id(&source.id)?;
    if let Some((owner, generation, active_switch)) = checkout_fence(tx, checkout_key)?
        && (owner != agent
            || active_switch
                .as_deref()
                .is_some_and(|active| active != switch_id))
    {
        return Err(Error::new(
            "bad_state",
            format!(
                "canonical checkout is owned by {}#{}{}",
                format_agent_id(owner),
                generation,
                active_switch
                    .map(|id| format!(" under switch {id}"))
                    .unwrap_or_default()
            ),
        ));
    }
    tx.execute(
        "INSERT INTO switch_checkout_owners(checkout_key, checkout_root, agent, generation,
             switch_command, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)
         ON CONFLICT(checkout_key) DO UPDATE SET checkout_root=excluded.checkout_root,
             agent=excluded.agent, generation=excluded.generation,
             switch_command=excluded.switch_command, updated_at=excluded.updated_at",
        params![
            checkout_key,
            checkout_root,
            agent,
            source.generation,
            switch_id,
            now
        ],
    )
    .map_err(sql_error)?;
    Ok(())
}

fn transfer_switch_checkout_fences(
    tx: &Transaction<'_>,
    switch_id: &str,
    source_key: Option<&str>,
    target_key: &str,
    target_root: &str,
    destination: &Agent,
    now: i64,
) -> Result<()> {
    let changed = tx
        .execute(
            "UPDATE switch_checkout_owners SET checkout_root=?1, agent=?2, generation=?3,
                 switch_command=NULL, updated_at=?4
             WHERE checkout_key=?5 AND switch_command=?6",
            params![
                target_root,
                parse_agent_id(&destination.id)?,
                destination.generation,
                now,
                target_key,
                switch_id
            ],
        )
        .map_err(sql_error)?;
    if changed != 1 {
        return Err(Error::new(
            "source_integrity",
            "switch target checkout fence was not retained",
        ));
    }
    if source_key.is_some_and(|source_key| source_key != target_key) {
        tx.execute(
            "DELETE FROM switch_checkout_owners WHERE checkout_key=?1 AND switch_command=?2",
            params![source_key, switch_id],
        )
        .map_err(sql_error)?;
    }
    Ok(())
}

fn retain_live_successor_checkout(
    tx: &Transaction<'_>,
    switch_id: &str,
    source_key: Option<&str>,
    target_key: &str,
    destination: &Agent,
    now: i64,
) -> Result<()> {
    tx.execute(
        "UPDATE switch_checkout_owners SET agent=?1, generation=?2, switch_command=NULL,
             updated_at=?3 WHERE checkout_key=?4 AND switch_command=?5",
        params![
            parse_agent_id(&destination.id)?,
            destination.generation,
            now,
            target_key,
            switch_id
        ],
    )
    .map_err(sql_error)?;
    if source_key.is_some_and(|source_key| source_key != target_key) {
        tx.execute(
            "UPDATE switch_checkout_owners SET switch_command=NULL, updated_at=?1
             WHERE checkout_key=?2 AND switch_command=?3",
            params![now, source_key, switch_id],
        )
        .map_err(sql_error)?;
    }
    Ok(())
}

fn release_switch_checkout_fences(
    tx: &Transaction<'_>,
    switch_id: &str,
    source_key: Option<&str>,
    target_key: Option<&str>,
    source: &Agent,
    now: i64,
) -> Result<()> {
    if let Some(target_key) = target_key
        && source_key != Some(target_key)
    {
        tx.execute(
            "DELETE FROM switch_checkout_owners WHERE checkout_key=?1 AND switch_command=?2",
            params![target_key, switch_id],
        )
        .map_err(sql_error)?;
    }
    if let Some(source_key) = source_key {
        tx.execute(
            "UPDATE switch_checkout_owners SET agent=?1, generation=?2,
                 switch_command=NULL, updated_at=?3
             WHERE checkout_key=?4 AND switch_command=?5",
            params![
                parse_agent_id(&source.id)?,
                source.generation,
                now,
                source_key,
                switch_id
            ],
        )
        .map_err(sql_error)?;
    }
    Ok(())
}

fn require_switch_source_exit(
    tx: &Connection,
    switch_id: &str,
    stop_id: &str,
    request: &SwitchRequest,
) -> Result<()> {
    let stop = component(tx, switch_id, stop_id, 3)?
        .ok_or_else(|| Error::new("source_integrity", "switch source stop is not retained"))?;
    if stop.verb != "stop"
        || stop.target.as_deref() != Some(request.from.as_str())
        || stop.generation != Some(request.from_generation)
        || stop.state != CommandState::Applied
    {
        return Err(Error::new(
            "process_custody_unproven",
            "switch source stop did not settle for the reserved generation",
        ));
    }
    let source = load_agent_by_num(tx, parse_agent_id(&request.from)?)?
        .ok_or_else(|| Error::new("not_found", "source Agent missing"))?;
    if source.generation != request.from_generation
        || !source.is_terminal()
        || handoff::session_exit_blocker(tx, &source)?.is_some()
    {
        return Err(Error::new(
            "process_custody_unproven",
            "switch requires committed process-family exit and native source drain proof",
        ));
    }
    Ok(())
}

fn continuation_directive(
    reason: HandoffReason,
    kind: Option<core::ProviderRestrictionKind>,
    source: &str,
    successor: &str,
    uncertain: bool,
) -> String {
    let why = match reason {
        HandoffReason::Request => format!("the work was moved from {source} to {successor}"),
        HandoffReason::Routing => format!("routing moved the work from {source} to {successor}"),
        HandoffReason::ProviderHold => format!(
            "{source} is on a provider {} hold",
            kind.map_or("quota", core::ProviderRestrictionKind::as_str)
        ),
        HandoffReason::SessionLost => format!("the {source} session was lost or failed"),
    };
    let caution = if uncertain {
        "\nThe previous turn stopped partway. Do not repeat any of its steps until Git and the files show that step did not take effect."
    } else {
        ""
    };
    format!(
        "Handoff brief: continue this task from its worktree. Why: {why}.{caution}\nYou now own this branch and its files as they are on disk, including uncommitted changes. The previous session and its context are gone. Git, the files and the Store facts below are the truth. The original task was written for the first session, so a step it limits to a first or earlier turn is already past. Read git status and git diff first, then finish the remaining work and commit at meaningful checkpoints on this branch. If the work is already complete, verify that and report it. Queued input, if any, arrives in separate messages after this one."
    )
}

pub(super) fn reserve_rule_switch_tx(
    tx: &Transaction<'_>,
    principal: &str,
    request: &SwitchRequest,
    limits: core::api::HierarchyLimits,
    policy: &core::SecretPolicy,
    now: i64,
) -> Result<Change<SwitchResponse>> {
    owner(principal)?;
    request.validate()?;
    let mut args = serde_json::to_value(request)
        .map_err(|_| Error::new("internal", "cannot encode rule switch"))?;
    let digest = canonical::digest(&args)?;
    args[OWNER_AUTOMATION] = json!(true);
    if policy.contains_value(&args) {
        return Err(Error::new(
            "secret_in_payload",
            "switch contains a known secret",
        ));
    }
    if encode_json(&args)?.len() > MAX_EVENT_BYTES {
        return Err(Error::new(
            "frame_too_large",
            "switch exceeds command budget",
        ));
    }
    if let Err(error) = source_tx(tx, request, limits, policy) {
        return if error.code == "busy" {
            Err(Error::new("bad_state", *error.message))
        } else {
            Err(error)
        };
    }
    reserve_switch_tx(tx, request, args, &digest, now, None, true)
}

fn reserve_switch_tx(
    tx: &Transaction<'_>,
    request: &SwitchRequest,
    mut args: Value,
    digest: &[u8],
    now: i64,
    live_checkout_snapshot: Option<&[(String, i64, String, String)]>,
    charge_switch: bool,
) -> Result<Change<SwitchResponse>> {
    let source = load_agent_by_num(tx, parse_agent_id(&request.from)?)?
        .ok_or_else(|| Error::new("not_found", "source Agent missing"))?;
    if source.generation != request.from_generation {
        return Err(Error::new("stale_generation", "source generation moved"));
    }
    let source_checkout_root = args
        .get(SOURCE_CHECKOUT_ROOT)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .or_else(|| source.workspace.clone());
    let source_checkout_key = args
        .get(SOURCE_CHECKOUT_KEY)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .or_else(|| {
            source_checkout_root
                .as_deref()
                .map(|root| core::workspace_path_key(root).into_owned())
        });
    args[SOURCE_CHECKOUT] = json!(source.workspace);
    args[SOURCE_CHECKOUT_ROOT] = json!(source_checkout_root);
    args[SOURCE_CHECKOUT_KEY] = json!(source_checkout_key);
    if let Some(error) = switch_blocker(tx, request, source_checkout_key.as_deref())? {
        return Err(error);
    }
    if charge_switch && !Store::charge_switch_reservation_tx(tx, &source, &request.id, now)? {
        return Err(Error::new(
            "switch_budget_unavailable",
            "the task continuation switch budget is exhausted or its previous reservation is unresolved",
        ));
    }
    if let (Some(checkout_key), Some(checkout_root)) = (
        source_checkout_key.as_deref(),
        source_checkout_root.as_deref(),
    ) {
        if let Some(snapshot) = live_checkout_snapshot {
            require_live_checkout_snapshot(tx, snapshot, checkout_key, &source.id)?;
        }
        claim_source_checkout(tx, checkout_key, checkout_root, &source, &request.id, now)?;
    }
    let value = SwitchResponse {
        id: request.id.clone(),
        from: request.from.clone(),
        from_generation: request.from_generation,
        state: CommandState::Accepted,
        launch_command: Ulid::new().to_string(),
        handoff_command: Ulid::new().to_string(),
        launch_state: None,
        created: None,
        handoff: None,
        error: None,
        error_reason: None,
        disposal_command: None,
        disposal_state: None,
        disposal_reason: None,
        routing: None,
        next_action: "preparing_launch".into(),
    };
    let mut parent = SubmitCommand::new(&request.id, "switch");
    parent.args = args;
    let outcome = serde_json::to_value(&value)
        .map_err(|_| Error::new("internal", "cannot encode switch result"))?;
    let event = admit_command(
        tx,
        &CommandInsert {
            method: METHOD,
            outcome: Some(&outcome),
            ..CommandInsert::targetless_admitted("owner", &parent, digest, now)
        },
        json!({"command_id":request.id,"verb":"switch"}),
    )?;
    Ok(Change {
        value,
        events: vec![event],
    })
}

/// Reserve the ordinary switch with the terminal fact, before publication.
/// The only replayable task is the original input of this role attempt.
pub(super) fn reserve_provider_switch_tx(
    tx: &Transaction<'_>,
    agent: &Agent,
    terminal: &core::NativeEvent,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    if terminal.kind != "turn.completed"
        || terminal.data.get("provider_request_refused_before_work") != Some(&Value::Bool(true))
        || terminal.data.get("outcome").and_then(Value::as_str) != Some("failure")
        || agent.generation != 1
        || agent.chat
        || agent.project.is_empty()
        || agent.observe_only
        || agent.parent.is_some()
        || agent.state != AgentState::Idle
    {
        return Ok(());
    }
    let Some(remaining) = agent
        .profile_choice
        .as_ref()
        .and_then(|choice| choice.provider_failover.as_ref())
    else {
        return Ok(());
    };
    let Some(task) = terminal.data.get("command_id").and_then(Value::as_str) else {
        return Ok(());
    };
    let Ok(restriction) = serde_json::from_value::<core::ProviderRestriction>(
        terminal.data["provider_restriction"].clone(),
    ) else {
        return Ok(());
    };
    if restriction.kind == core::ProviderRestrictionKind::ModelRemoved
        || !provider_retry_allowed(tx, agent, task)?
    {
        return Ok(());
    }
    match handoff::quiescent(tx, agent, false) {
        Ok(()) => {}
        Err(error) if error.code == "busy" => return Ok(()),
        Err(error) => return Err(error),
    }
    let agent_num = parse_agent_id(&agent.id)?;
    if remaining.is_empty() {
        events.push(insert_daemon_event(
            tx,
            now,
            PROVIDER_SWITCH_SETTLED,
            "provider",
            Some(agent_num),
            Some(agent.generation),
            json!({"task_command":task, "kind":restriction.kind, "state":"exhausted"}),
        )?);
        return Ok(());
    }
    let request = SwitchRequest {
        id: Ulid::new().to_string(),
        from: agent.id.clone(),
        from_generation: agent.generation,
        task_command: task.to_owned(),
        commands: Vec::new(),
        launch: SwitchLaunch::default(),
    };
    let checkout_key = agent.workspace.as_deref().map(core::workspace_path_key);
    if switch_blocker(tx, &request, checkout_key.as_deref())?.is_some() {
        return Ok(());
    }
    let mut args = serde_json::to_value(&request)
        .map_err(|_| Error::new("internal", "cannot encode provider switch"))?;
    args[PROVIDER_FAILURE] = json!(restriction.kind);
    args[PROVIDER_AUTHORITY] = json!(core::provider_switch_authority(&agent.profile_snapshot)?);
    if agent
        .profile_choice
        .as_ref()
        .is_some_and(|choice| choice.owner_automation)
        || command_cause_tx(tx, task)?.is_some_and(|cause| cause.rule.is_some())
    {
        args[OWNER_AUTOMATION] = json!(true);
    }
    let digest = canonical::digest(&args)?;
    let change = match reserve_switch_tx(tx, &request, args, &digest, now, None, true) {
        Ok(change) => change,
        Err(error) if error.code == "switch_budget_unavailable" => {
            events.push(insert_daemon_event(
                tx,
                now,
                PROVIDER_SWITCH_SETTLED,
                "provider",
                Some(agent_num),
                Some(agent.generation),
                json!({
                    "task_command": task,
                    "kind": restriction.kind,
                    "state": "budget_unavailable",
                }),
            )?);
            return Ok(());
        }
        Err(error) => return Err(error),
    };
    events.extend(change.events);
    events.push(insert_daemon_event(
        tx,
        now,
        PROVIDER_SWITCH_QUEUED,
        "provider",
        Some(agent_num),
        Some(agent.generation),
        json!({"switch_command":request.id, "task_command":task, "kind":restriction.kind}),
    )?);
    Ok(())
}

fn require_provider_retry(tx: &Transaction<'_>, agent: &Agent, task: &str) -> Result<()> {
    if !provider_retry_allowed(tx, agent, task)? {
        return Err(Error::new("bad_state", "provider retry source changed"));
    }
    Ok(())
}

fn provider_retry_allowed(tx: &Transaction<'_>, agent: &Agent, task: &str) -> Result<bool> {
    let num = parse_agent_id(&agent.id)?;
    tx.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM commands c
            JOIN commands p ON p.id=c.parent_command
            JOIN turns t ON t.agent=c.target AND t.generation=c.generation AND t.turn_id=c.turn_id
            WHERE c.id=?1 AND c.target=?2 AND c.generation=?3 AND c.principal='owner'
              AND c.verb='send' AND c.state='applied' AND c.ordinal=0
              AND ((c.link_kind='send' AND p.verb='launch' AND p.target=c.target
                  AND NOT EXISTS(SELECT 1 FROM json_each(p.args) field
                    JOIN json_each(?4) explicit ON explicit.value=field.key))
                OR (c.link_kind='handoff' AND p.method='swarm.handoff' AND EXISTS(
                    SELECT 1 FROM commands s
                    JOIN commands l ON l.parent_command=s.id AND l.ordinal=0 AND l.target=c.target
                    WHERE s.id=p.parent_command AND s.method='swarm.switch'
                      AND json_extract(s.args,'$.provider_failure') IS NOT NULL)))
              AND t.state='completed' AND t.outcome='failure' AND t.tool_calls=0
              AND t.final_text IS NULL
              AND EXISTS(SELECT 1 FROM source_state ss WHERE ss.agent=?2 AND ss.generation=?3
                AND ss.source='stream' AND ss.state='open' AND ss.health='ok' AND ss.gap_id IS NULL)
              AND NOT EXISTS(SELECT 1 FROM commands other WHERE other.target=?2 AND other.generation=?3
                AND other.id<>c.id AND other.verb<>'launch')
        )",
        params![task, num, agent.generation, encode_json(&core::PROVIDER_LAUNCH_OVERRIDE_FIELDS)?],
        |row| row.get(0),
    ).map_err(sql_error)
}

impl Store {
    pub fn accepted_provider_switches(&self, after: &str) -> Result<Vec<String>> {
        self.ensure_committed_reads()?;
        let mut statement = self
            .connection
            .prepare(
                "SELECT id FROM commands WHERE state='accepted' AND verb='switch' AND method=?1
                 AND (json_extract(args,'$.provider_failure') IS NOT NULL
                      OR json_extract(args,'$.continuation') IS NOT NULL) AND id > ?2
                 ORDER BY id LIMIT 64",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map(params![METHOD, after], |row| row.get(0))
            .map_err(sql_error)?;
        rows.collect::<rusqlite::Result<_>>().map_err(sql_error)
    }

    pub fn replay_switch(
        &self,
        principal: &str,
        request: &SwitchRequest,
    ) -> Result<Option<SwitchResponse>> {
        self.ensure_committed_reads()?;
        request.validate()?;
        require_switch_principal(principal)?;
        let (_, digest) = switch_args(principal, request)?;
        replay_on(&self.connection, "owner", request, &digest)
    }

    /// Current non-observer writer workspaces. Router resolves each path to
    /// its canonical Git checkout before switch reservation or launch.
    pub fn switch_live_workspace_inputs(&self) -> Result<Vec<(String, i64, String)>> {
        self.ensure_committed_reads()?;
        live_workspace_inputs(&self.connection)
    }

    /// Retain both child identities before any profile/auth/Git/native preparation.
    pub fn reserve_switch(
        &mut self,
        principal: &str,
        request: &SwitchRequest,
        policy: &core::SecretPolicy,
        reservation: SwitchReservation<'_>,
        now: i64,
    ) -> Result<Change<SwitchResponse>> {
        let SwitchReservation {
            owner_process,
            profile_authority,
            source_checkout,
            live_checkouts,
        } = reservation;
        let charge_switch = principal != "owner" || profile_authority.is_some();
        if let Some(value) = self.replay_switch(principal, request)? {
            return Ok(Change {
                value,
                events: Vec::new(),
            });
        }
        self.ensure_writer_healthy()?;
        let result = (|| {
            let (mut args, digest) = switch_args(principal, request)?;
            if let Some((root, key)) = source_checkout {
                args[SOURCE_CHECKOUT_ROOT] = json!(root);
                args[SOURCE_CHECKOUT_KEY] = json!(key);
            }
            if let Some(authority) = profile_authority {
                args[SESSION_GROWTH_AUTHORITY] = json!(authority);
            }
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            if let Some(value) = replay_on(&tx, "owner", request, &digest)? {
                commit_transaction(tx, &self.write_faulted)?;
                return Ok(Change {
                    value,
                    events: Vec::new(),
                });
            }
            validate_id_age(&request.id, now)?;
            if policy.contains_value(&args) {
                return Err(Error::new(
                    "secret_in_payload",
                    "switch contains a known secret",
                ));
            }
            if encode_json(&args)?.len() > MAX_EVENT_BYTES {
                return Err(Error::new(
                    "frame_too_large",
                    "switch exceeds command budget",
                ));
            }
            require_switch_caller(&tx, principal, request)?;
            source_tx(&tx, request, self.hierarchy_limits, policy)?;
            let mut change = reserve_switch_tx(
                &tx,
                request,
                args,
                &digest,
                now,
                Some(live_checkouts),
                charge_switch,
            )?;
            annotate_owner_events(&tx, &mut change.events, owner_process)?;
            commit_transaction(tx, &self.write_faulted)?;
            Ok(change)
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    /// Claim one worker once. Retries are readback, not continuation or another launch.
    pub fn start_switch(&mut self, id: &str, now: i64) -> Result<Change<Option<SwitchWork>>> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let command = switch_parent(&tx, id)?;
            if command.state != CommandState::Accepted {
                return Ok(Change {
                    value: None,
                    events: Vec::new(),
                });
            }
            let mut events = Vec::new();
            if !matches!(
                rules::gate_rule_action_dispatch_tx(&tx, id, now, &mut events)?,
                rules::RuleActionDispatchGate::Ready
            ) {
                commit_transaction(tx, &self.write_faulted)?;
                return Ok(Change {
                    value: None,
                    events,
                });
            }
            let value = stored_result(&command.outcome)?;
            let RetainedSwitch {
                request,
                source_stop_command: _,
                source_checkout,
                source_checkout_root,
                source_checkout_key,
                target_checkout_root: _,
                target_checkout_key: _,
                provider_failure,
                profile_authority,
                owner_automation,
                continuation,
                requested_by,
            } = decode_switch(command.args)?;
            let owner_automation = owner_automation
                || continuation.is_some()
                || requested_by.is_some()
                || profile_authority.is_some()
                || command_cause_tx(&tx, id)?.is_some_and(|cause| cause.rule.is_some())
                || (provider_failure.is_some()
                    && command_cause_tx(&tx, &request.task_command)?
                        .is_some_and(|cause| cause.rule.is_some()))
                || tx
                    .query_row(
                        "SELECT EXISTS(SELECT 1 FROM events
                      WHERE agent=?1 AND generation=?2
                        AND kind='x.user.agent.attention_step'
                        AND json_extract(data,'$.step')='switch_for_session_growth'
                        AND json_extract(data,'$.command_id')=?3)",
                        params![parse_agent_id(&request.from)?, request.from_generation, id],
                        |row| row.get::<_, bool>(0),
                    )
                    .map_err(sql_error)?;
            let source = load_agent_by_num(&tx, parse_agent_id(&request.from)?)?
                .ok_or_else(|| Error::new("not_found", "source Agent missing"))?;
            if source.generation != request.from_generation {
                return Err(Error::new("stale_generation", "source generation moved"));
            }
            if source.workspace != source_checkout {
                return Err(Error::new(
                    "source_integrity",
                    "source canonical checkout changed after switch reservation",
                ));
            }
            if let (Some(root), Some(key)) = (
                source_checkout_root.as_deref(),
                source_checkout_key.as_deref(),
            ) {
                if core::workspace_path_key(root) != key {
                    return Err(Error::new(
                        "source_integrity",
                        "reserved source checkout key does not match its canonical root",
                    ));
                }
                let source_num = parse_agent_id(&source.id)?;
                if checkout_fence(&tx, key)?.is_none_or(|(owner, generation, active)| {
                    owner != source_num
                        || generation != source.generation
                        || active.as_deref() != Some(id)
                }) {
                    return Err(Error::new(
                        "bad_state",
                        "source checkout ownership changed after reservation",
                    ));
                }
            }
            let source_stop_command = Ulid::new().to_string();
            let mut stop = SubmitCommand::new(&source_stop_command, "stop");
            stop.target = Some(source.id.clone());
            stop.expected_generation = Some(source.generation);
            stop.args = json!({"force": true});
            let admitted = admission::submit_tx(
                &tx,
                "owner",
                stop,
                None,
                now,
                false,
                self.hierarchy_limits,
                self.capacity_policy.as_deref(),
            )?;
            tx.execute(
                "UPDATE commands SET args=json_set(args, '$.switch_source_stop_command', ?1)
                 WHERE id=?2",
                params![source_stop_command, id],
            )
            .map_err(sql_error)?;
            tx.execute(
                "UPDATE commands SET parent_command=?1,link_kind='switch',ordinal=3 WHERE id=?2",
                params![id, source_stop_command],
            )
            .map_err(sql_error)?;
            events.extend(admitted.events);
            update_command_state(
                &tx,
                id,
                CommandState::Dispatched,
                command.outcome.as_ref(),
                None,
                now,
            )?;
            events.push(insert_daemon_command_event(
                &tx,
                now,
                "command.dispatched",
                None,
                None,
                id,
                json!({"component":"switch_orchestration"}),
            )?);
            commit_transaction(tx, &self.write_faulted)?;
            Ok(Change {
                value: Some(SwitchWork {
                    request,
                    plan: value,
                    source_stop_command: Some(source_stop_command),
                    source_checkout,
                    source_checkout_root,
                    source_checkout_key,
                    provider_failure,
                    profile_authority,
                    owner_automation,
                    continuation,
                    requested_by,
                }),
                events,
            })
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn switch_launch_ready(&self, id: &str) -> Result<bool> {
        self.ensure_committed_reads()?;
        let value = result_on(&self.connection, id)?;
        let Some(launch) = load_command(&self.connection, &value.launch_command)? else {
            return Ok(false);
        };
        if launch.state != CommandState::Applied {
            return Ok(false);
        }
        match ready_destination_tx(&self.connection, &launch) {
            Ok(_) => Ok(true),
            Err(error) if error.code == "internal" => Err(error),
            Err(_) => Ok(false),
        }
    }

    /// Whether the reserved source stop completed with exact process-family
    /// exit and native source-drain proof for this switch generation.
    pub fn switch_source_exit_proven(&self, id: &str) -> Result<bool> {
        self.ensure_committed_reads()?;
        let parent = switch_parent(&self.connection, id)?;
        let retained = decode_switch(parent.args)?;
        let Some(stop_id) = retained.source_stop_command.as_deref() else {
            return Ok(false);
        };
        match require_switch_source_exit(&self.connection, id, stop_id, &retained.request) {
            Ok(()) => Ok(true),
            Err(error) if error.code == "process_custody_unproven" => Ok(false),
            Err(error) => Err(error),
        }
    }

    pub fn switch_result(&self, id: &str) -> Result<SwitchResponse> {
        self.ensure_committed_reads()?;
        result_on(&self.connection, id)
    }

    /// Same transaction as normal launch allocation, capacity/workspace reservation and child link.
    pub fn admit_switch_launch(
        &mut self,
        id: &str,
        request: SubmitCommand,
        preparation: Option<WorkspacePreparationInput>,
        checkout: SwitchLaunchCheckout<'_>,
        policy: &core::SecretPolicy,
        now: i64,
    ) -> Result<Admission> {
        let SwitchLaunchCheckout {
            root: target_checkout_root,
            key: target_checkout_key,
            live_checkouts: live_checkout_snapshot,
            source_clean: source_checkout_clean,
        } = checkout;
        self.ensure_writer_healthy()?;
        let generated_target = preparation
            .as_ref()
            .is_some_and(|input| input.generated.is_some());
        let result = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let parent = switch_parent(&tx, id)?;
            let plan = stored_result(&parent.outcome)?;
            if parent.state != CommandState::Dispatched
                || request.id != plan.launch_command
                || request.verb != "launch"
                || request.args.get("text").is_some()
                || request.args.get("prompt").is_some()
                || request.args.get("parent").is_some()
            {
                return Err(Error::new(
                    "bad_state",
                    "switch launch binding or phase differs",
                ));
            }
            let mut retained_args = parent.args.clone();
            let RetainedSwitch {
                request: source,
                source_stop_command,
                source_checkout,
                source_checkout_root,
                source_checkout_key,
                provider_failure,
                continuation,
                requested_by,
                ..
            } = decode_switch(parent.args.clone())?;
            let (agent, target) = source_tx(&tx, &source, self.hierarchy_limits, policy)?;
            if agent.workspace != source_checkout {
                return Err(Error::new(
                    "source_integrity",
                    "source canonical checkout changed after switch reservation",
                ));
            }
            let source_checkout_key = source_checkout_key.as_deref().ok_or_else(|| {
                Error::new(
                    "source_integrity",
                    "switch source checkout identity is missing",
                )
            })?;
            let source_checkout_root = source_checkout_root.as_deref().ok_or_else(|| {
                Error::new("source_integrity", "switch source checkout root is missing")
            })?;
            if core::workspace_path_key(source_checkout_root) != source_checkout_key {
                return Err(Error::new(
                    "source_integrity",
                    "reserved source checkout key does not match its canonical root",
                ));
            }
            let source_num = parse_agent_id(&agent.id)?;
            if checkout_fence(&tx, source_checkout_key)?.is_none_or(
                |(owner, generation, active)| {
                    owner != source_num
                        || generation != agent.generation
                        || active.as_deref() != Some(id)
                },
            ) {
                return Err(Error::new(
                    "bad_state",
                    "source checkout ownership changed before launch admission",
                ));
            }
            let source_stop = source_stop_command.as_deref().ok_or_else(|| {
                Error::new(
                    "process_custody_unproven",
                    "switch has no reserved source stop command",
                )
            })?;
            require_switch_source_exit(&tx, id, source_stop, &source)?;
            if continuation.is_some() || requested_by.is_some() {
                require_handoff_bounds(
                    &tx,
                    &agent,
                    &request.args["profile_snapshot"],
                    request.args.get("workspace").and_then(Value::as_str),
                    request.args.get("profile").and_then(Value::as_str),
                    requested_by.as_deref(),
                )?;
            }
            if let Some(held_since) = continuation.and_then(|value| value.held_since)
                && !automatic::hold_is_current(&tx, &agent, held_since)?
            {
                return Err(Error::new(
                    "bad_state",
                    "the provider hold changed before the handoff",
                ));
            }
            if let Some(failure) = provider_failure {
                require_provider_retry(&tx, &agent, &source.task_command)?;
                failure.require_authority(&agent.profile_snapshot)?;
                failure.require_authority(&request.args["profile_snapshot"])?;
                let selected = request.args.get("profile").and_then(Value::as_str);
                if !agent
                    .profile_choice
                    .as_ref()
                    .and_then(|choice| choice.provider_failover.as_ref())
                    .is_some_and(|remaining| {
                        remaining.iter().any(|name| Some(name.as_str()) == selected)
                    })
                {
                    return Err(Error::new(
                        "bad_state",
                        "provider switch selection is outside the frozen continuation list",
                    ));
                }
            }
            let requested = serde_json::to_value(&source.launch)
                .map_err(|_| Error::new("internal", "cannot encode switch selectors"))?;
            for key in [
                "profile", "harness", "model", "effort", "auth", "cmd", "worktree", "base",
            ] {
                if let Some(value) = requested.get(key).filter(|value| !value.is_null())
                    && request.args.get(key) != Some(value)
                {
                    return Err(Error::new(
                        "id_conflict",
                        "prepared launch differs from reserved selectors",
                    ));
                }
            }

            if request.args.get("project").and_then(Value::as_str) != Some(agent.project_scope()) {
                return Err(Error::new(
                    "not_allowed",
                    "switch launch must keep the source project",
                ));
            }
            // Continue-in-project keeps the source authorization scope above
            // and additionally binds the resolved execution registration: the
            // prepared launch must carry the target the worker verified, at
            // the revision it verified, or a mismatched workspace or a
            // revoked registration refuses inside this transaction.
            if let Some(registration) = target {
                if request
                    .args
                    .get("registered_project")
                    .and_then(Value::as_str)
                    != Some(registration.id.as_str())
                {
                    return Err(Error::new(
                        "not_allowed",
                        "switch target project differs from prepared registration",
                    ));
                }
                let revision = request
                    .args
                    .get(core::api::SWITCH_PROJECT_REVISION_ARG)
                    .and_then(Value::as_i64);
                if revision != Some(registration.revision) {
                    return Err(Error::new(
                        "stale_assignment",
                        "switch target registration changed",
                    ));
                }
                // The frozen checkout admitted here must already be the
                // target registration's checkout: a changed registration or
                // a preparation from another repository refuses before any
                // workspace row or Agent is created.
                if request.args.get("workspace").and_then(Value::as_str)
                    != Some(registration.checkout.as_str())
                {
                    return Err(Error::new(
                        "not_allowed",
                        "switch workspace differs from its target registration",
                    ));
                }
                if let Some(input) = preparation.as_ref()
                    && (input.repo_path != registration.checkout
                        || input.repo_common_dir != registration.repo_common_dir
                        || input.project != registration.checkout)
                {
                    return Err(Error::new(
                        "not_allowed",
                        "switch workspace preparation differs from its target registration",
                    ));
                }
                if let Some(preparation) = request.args.get("workspace_preparation") {
                    let repo_path = preparation.get("repo_path").and_then(Value::as_str);
                    let repo_common_dir =
                        preparation.get("repo_common_dir").and_then(Value::as_str);
                    let preparation_project = preparation.get("project").and_then(Value::as_str);
                    if repo_path != Some(registration.checkout.as_str())
                        || repo_common_dir != Some(registration.repo_common_dir.as_str())
                        || preparation_project != Some(registration.checkout.as_str())
                    {
                        return Err(Error::new(
                            "not_allowed",
                            "switch workspace preparation differs from its target registration",
                        ));
                    }
                }
            } else if request
                .args
                .get("registered_project")
                .and_then(Value::as_str)
                .is_some_and(|project| project != agent.project)
                || (agent.project != agent.project_scope()
                    && request
                        .args
                        .get("registered_project")
                        .and_then(Value::as_str)
                        != Some(agent.project.as_str()))
            {
                // An ordinary switch preserves both the execution project and
                // the authorization scope.
                return Err(Error::new(
                    "not_allowed",
                    "switch must keep the source execution project",
                ));
            }
            if load_command(&tx, &request.id)?.is_some()
                && component(&tx, id, &request.id, 0)?.is_none()
            {
                return Err(Error::new(
                    "id_conflict",
                    "reserved launch id occupied by another command",
                ));
            }
            super::work_truth::require_no_checkpoint_intent_tx(&tx, &agent)?;
            let admission = admission::submit_switch_launch_tx(
                &tx,
                "owner",
                request,
                preparation,
                now,
                false,
                self.hierarchy_limits,
                self.capacity_policy.as_deref(),
            )?;
            let target_num =
                parse_agent_id(admission.command.target.as_deref().ok_or_else(|| {
                    Error::new("source_integrity", "switch launch Agent is missing")
                })?)?;
            let target_agent = load_agent_by_num(&tx, target_num)?
                .ok_or_else(|| Error::new("source_integrity", "switch launch Agent is missing"))?;
            let (actual_checkout_root, actual_checkout_key) = if generated_target {
                let root = target_agent.workspace.clone().ok_or_else(|| {
                    Error::new("source_integrity", "generated switch checkout is missing")
                })?;
                let key = core::workspace_path_key(&root).into_owned();
                (root, key)
            } else {
                (
                    target_checkout_root.to_owned(),
                    target_checkout_key.to_owned(),
                )
            };
            let same_checkout = source_checkout_key == actual_checkout_key.as_str();
            if !same_checkout && source_checkout_clean != Some(true) {
                return Err(Error::new(
                    "dirty",
                    "switch to a different checkout requires a fully verified clean source worktree",
                ));
            }
            require_live_checkout_snapshot(
                &tx,
                live_checkout_snapshot,
                &actual_checkout_key,
                &agent.id,
            )?;
            claim_source_checkout(
                &tx,
                &actual_checkout_key,
                &actual_checkout_root,
                &agent,
                id,
                now,
            )?;
            retained_args[TARGET_CHECKOUT_ROOT] = json!(actual_checkout_root);
            retained_args[TARGET_CHECKOUT_KEY] = json!(actual_checkout_key);
            tx.execute(
                "UPDATE commands SET args=?1 WHERE id=?2",
                params![encode_json(&retained_args)?, id],
            )
            .map_err(sql_error)?;
            tx.execute(
                "UPDATE commands SET parent_command=?1,link_kind='switch',ordinal=0 WHERE id=?2",
                params![id, plan.launch_command],
            )
            .map_err(sql_error)?;
            commit_transaction(tx, &self.write_faulted)?;
            Ok(admission)
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    /// Commit the H2 transfer, or roll back H2 and admit ordinary disposal of
    /// the exact unused successor after a definite refusal.
    pub fn finish_switch(
        &mut self,
        id: &str,
        policy: &core::SecretPolicy,
        facts: &handoff::BriefFacts,
        now: i64,
    ) -> Result<Change<SwitchResponse>> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let parent = switch_parent(&tx, id)?;
            if parent.state != CommandState::Dispatched {
                let value = result_on(&tx, id)?;
                return Ok(Change {
                    value,
                    events: Vec::new(),
                });
            }
            let RetainedSwitch {
                request,
                source_checkout_key,
                target_checkout_root,
                target_checkout_key,
                provider_failure,
                continuation,
                requested_by,
                ..
            } = decode_switch(parent.args)?;
            let handoff_reason = continuation.map_or(
                if request.launch.role.is_some() {
                    HandoffReason::Routing
                } else {
                    HandoffReason::Request
                },
                |value| value.reason,
            );
            let mut uncertain = false;
            let plan = stored_result(&parent.outcome)?;
            let launch = component(&tx, id, &plan.launch_command, 0)?
                .ok_or_else(|| Error::new("bad_state", "switch launch not admitted"))?;
            let mut events = Vec::new();
            let mut state = CommandState::Failed;
            let error;
            let mut disposal = None;
            let transfer: Result<handoff::HandoffAdmission> = if launch.state
                == CommandState::Applied
            {
                tx.execute_batch("SAVEPOINT switch_transfer")
                    .map_err(sql_error)?;
                (|| {
                    let destination = ready_destination_tx(&tx, &launch)?;
                    let handoff = request.handoff(
                        &plan.handoff_command,
                        &destination.id,
                        destination.generation,
                    );
                    let (source, _) = source_tx(&tx, &request, self.hierarchy_limits, policy)?;
                    if let Some(failure) = provider_failure {
                        require_provider_retry(&tx, &source, &request.task_command)?;
                        failure.require_authority(&source.profile_snapshot)?;
                        failure.require_authority(&destination.profile_snapshot)?;
                    }
                    if continuation.is_some() || requested_by.is_some() {
                        require_handoff_bounds(
                            &tx,
                            &source,
                            &destination.profile_snapshot,
                            destination.workspace.as_deref(),
                            Some(destination.profile.as_str()),
                            requested_by.as_deref(),
                        )?;
                    }
                    let source_num = parse_agent_id(&request.from)?;
                    uncertain =
                        handoff::last_turn_uncertain(&tx, source_num, request.from_generation)?
                            || has_unknown_gate(&tx, source_num)?;
                    let directive = continuation_directive(
                        handoff_reason,
                        continuation.and_then(|value| value.kind),
                        &source.profile,
                        &destination.profile,
                        uncertain,
                    );
                    let transfer = if provider_failure.is_some() {
                        handoff::HandoffInput::ProviderRetry
                    } else if request.launch.project.is_some() {
                        // Continuing a chat in a project waits for the Owner.
                        handoff::HandoffInput::Brief {
                            facts,
                            discover: true,
                        }
                    } else {
                        handoff::HandoffInput::Continuation {
                            directive: &directive,
                            facts,
                        }
                    };
                    let change = handoff::handoff_tx(
                        &tx,
                        "owner",
                        &handoff,
                        policy,
                        now,
                        self.hierarchy_limits,
                        transfer,
                    )?;
                    super::work_truth::inherit_work_task_tx(&tx, &source, &destination, now)?;
                    Store::transfer_handoff_lineage_tx(&tx, &source, &destination)?;
                    let target_root = target_checkout_root.as_deref().ok_or_else(|| {
                        Error::new("source_integrity", "switch target checkout root is missing")
                    })?;
                    let target_key = target_checkout_key.as_deref().ok_or_else(|| {
                        Error::new("source_integrity", "switch target checkout key is missing")
                    })?;
                    transfer_switch_checkout_fences(
                        &tx,
                        id,
                        source_checkout_key.as_deref(),
                        target_key,
                        target_root,
                        &destination,
                        now,
                    )?;
                    tx.execute("UPDATE commands SET parent_command=?1,link_kind='switch',ordinal=1 WHERE id=?2", params![id,plan.handoff_command]).map_err(sql_error)?;
                    // The continuation rides the existing overlay: B's
                    // link records the source it continues. Plain text,
                    // never a foreign key, so deleting the source
                    // afterwards cannot fault the Store writer.
                    let created = parse_agent_id(&destination.id)?;
                    // Persist ownership for the actual canonical checkout,
                    // including an explicit workspace that names the same path.
                    let same_checkout = source_checkout_key
                        .as_deref()
                        .zip(target_checkout_key.as_deref())
                        .is_some_and(|(source, target)| source == target);
                    if same_checkout {
                        db!(tx.execute(
                            "INSERT OR REPLACE INTO agent_successors(agent, generation, successor)
                             VALUES (?1, ?2, ?3)",
                            params![
                                parse_agent_id(&request.from)?,
                                request.from_generation,
                                created
                            ],
                        ));
                    }
                    // An existing row (a menu write raced the handoff)
                    // moves to a new revision like any overlay write, so
                    // a menu that read it before the switch is refused.
                    db!(tx.execute(
                            "INSERT INTO chat_organization(agent, project_link, tags, continued_from)
                             VALUES (?1, NULL, '[]', ?2)
                             ON CONFLICT(agent) DO UPDATE SET continued_from = excluded.continued_from,
                                 revision = chat_organization.revision + 1",
                            params![created, request.from],
                        ));
                    // The event names B's row as this write left it, so a
                    // loaded sidebar refreshes B by id at its revision.
                    let (organization, tags, revision) =
                        organization::agent_organization(&tx, created)?;
                    let organization_change = OrganizationChangedEvent {
                        id: id.to_owned(),
                        method: METHOD.to_owned(),
                        rows: vec![OrgRowChange {
                            agent: format_agent_id(created),
                            organization,
                            tags,
                            revision: revision.ok_or_else(|| {
                                Error::new("internal", "switch overlay row is missing")
                            })?,
                        }],
                    };
                    organization_change.validate()?;
                    let mut change = change;
                    change.events.push(insert_daemon_event(
                        &tx,
                        now,
                        ORGANIZATION_CHANGED_KIND,
                        "organization",
                        None,
                        None,
                        serde_json::to_value(&organization_change).map_err(|_| {
                            Error::new("internal", "cannot encode organization event")
                        })?,
                    )?);
                    // B takes A's place under A's live manager.
                    if let Some(parent_id) = source.parent.as_deref()
                        && let Some(manager) = load_agent_by_num(&tx, parse_agent_id(parent_id)?)?
                        && !manager.is_terminal()
                    {
                        let moved = reparent::reparent_tx(
                            &tx,
                            &ReparentRequest {
                                id: plan.handoff_command.clone(),
                                agent: Some(destination.id.clone()),
                                children_of: None,
                                expected_generation: destination.generation,
                                parent: manager.id.clone(),
                                parent_generation: manager.generation,
                            },
                            self.hierarchy_limits,
                            now,
                            true,
                        )?;
                        change.events.extend(moved.events);
                    }
                    Ok(change)
                })()
            } else {
                Err(Error::new(
                    if matches!(
                        launch.state,
                        CommandState::Accepted
                            | CommandState::Queued
                            | CommandState::Dispatched
                            | CommandState::Unknown
                    ) {
                        "unknown"
                    } else {
                        "launch_not_ready"
                    },
                    "launch has no proven readiness",
                ))
            };
            let mut disposal_reason = None;
            match transfer {
                Ok(change) => {
                    tx.execute_batch("RELEASE switch_transfer")
                        .map_err(sql_error)?;
                    events.extend(change.events);
                    state = CommandState::Applied;
                    error = None;
                }
                Err(refusal) => {
                    if launch.state == CommandState::Applied {
                        // A successful rollback proves even an internal H2 failure had no transfer effect.
                        tx.execute_batch("ROLLBACK TO switch_transfer; RELEASE switch_transfer")
                            .map_err(sql_error)?;
                    }
                    if refusal.code == "unknown" {
                        state = CommandState::Unknown;
                    } else if let Some(destination) = launch_destination_tx(&tx, &launch)?
                        && switch_successor_needs_stop(&tx, &destination)?
                    {
                        if switch_successor_has_input(&tx, &destination)? {
                            disposal_reason = Some(
                                "Successor has a SEND or turn. Its session was left running."
                                    .to_owned(),
                            );
                        } else {
                            let mut stop = SubmitCommand::new(Ulid::new().to_string(), "stop");
                            stop.target = Some(destination.id);
                            stop.expected_generation = Some(destination.generation);
                            stop.args = json!({"force": true});
                            let admitted = admission::submit_tx(
                                &tx,
                                "owner",
                                stop,
                                None,
                                now,
                                false,
                                self.hierarchy_limits,
                                self.capacity_policy.as_deref(),
                            )?;
                            tx.execute(
                                "UPDATE commands SET parent_command=?1,link_kind='switch',ordinal=2 WHERE id=?2",
                                params![id, admitted.command.id],
                            ).map_err(sql_error)?;
                            events.extend(admitted.events);
                            disposal = Some(admitted.command);
                        }
                    }
                    if refusal.code != "unknown" && disposal.is_none() {
                        if disposal_reason.is_some() {
                            if let Some(destination) = launch_destination_tx(&tx, &launch)? {
                                let target_key =
                                    target_checkout_key.as_deref().ok_or_else(|| {
                                        Error::new(
                                            "source_integrity",
                                            "switch target checkout key is missing",
                                        )
                                    })?;
                                retain_live_successor_checkout(
                                    &tx,
                                    id,
                                    source_checkout_key.as_deref(),
                                    target_key,
                                    &destination,
                                    now,
                                )?;
                            }
                        } else if let Some(source) =
                            load_agent_by_num(&tx, parse_agent_id(&request.from)?)?
                        {
                            release_switch_checkout_fences(
                                &tx,
                                id,
                                source_checkout_key.as_deref(),
                                target_checkout_key.as_deref(),
                                &source,
                                now,
                            )?;
                        }
                    }
                    error = Some(refusal);
                }
            }
            let mut value = result_on(&tx, id)?;
            if let Some(error) = &error {
                set_switch_error(&mut value, error, policy);
            }
            value.state = state;
            value.disposal_reason = disposal_reason;
            if let Some(disposal) = disposal {
                value.disposal_command = Some(disposal.id);
                value.disposal_state = Some(disposal.state);
            }
            value.next_action = switch_next_action(&value).into();
            // H2 owns the full transfer result; keep only its fixed reference here.
            value.handoff = None;
            let outcome = serde_json::to_value(&value)
                .map_err(|_| Error::new("internal", "cannot encode switch result"))?;
            update_command_state(&tx, id, state, Some(&outcome), None, now)?;
            Store::settle_automatic_handoff_attempt_tx(&tx, id, state, now)?;
            events.push(insert_daemon_command_event(
                &tx, now, &format!("command.{}",command_state_name(state)), None, None, id,
                json!({"launch_command":plan.launch_command,"handoff_command":plan.handoff_command,
                    "error":value.error,"reason":value.error_reason,"disposal_command":value.disposal_command}),
            )?);
            let value = result_on(&tx, id)?;
            if let Some(failure) = provider_failure {
                let kind = failure.kind;
                events.push(insert_daemon_event(
                    &tx, now, PROVIDER_SWITCH_SETTLED, "provider",
                    Some(parse_agent_id(&request.from)?), Some(request.from_generation),
                    json!({
                        "switch_command": id,
                        "task_command": request.task_command,
                        "kind": kind,
                        "continued_to": value.created.as_ref().filter(|_| state == CommandState::Applied).map(|agent| &agent.agent),
                        "profile": launch.args.get("profile"),
                        "retry_command": value.handoff.as_ref().map(|handoff| &handoff.brief_command),
                        "error": value.error,
                        "state": state,
                    }),
                )?);
            } else {
                events.push(insert_daemon_event(
                    &tx,
                    now,
                    HANDOFF_SETTLED,
                    "handoff",
                    Some(parse_agent_id(&request.from)?),
                    Some(request.from_generation),
                    json!({
                        "switch_command": id,
                        "task_command": request.task_command,
                        "reason": handoff_reason,
                        "kind": continuation.and_then(|value| value.kind),
                        "requested_by": requested_by,
                        "continued_to": value.created.as_ref()
                            .filter(|_| state == CommandState::Applied)
                            .map(|agent| &agent.agent),
                        "profile": launch.args.get("profile"),
                        "uncertain": uncertain && state == CommandState::Applied,
                        "error": value.error,
                        "state": state,
                    }),
                )?);
            }
            commit_transaction(tx, &self.write_faulted)?;
            Ok(Change { value, events })
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn fail_switch_preparation(
        &mut self,
        id: &str,
        error: &Error,
        policy: &core::SecretPolicy,
        now: i64,
    ) -> Result<Change<Command>> {
        let parent = switch_parent(&self.connection, id)?;
        let retained = decode_switch(parent.args)?;
        let source = load_agent_by_num(&self.connection, parse_agent_id(&retained.request.from)?)?;
        if let Some(source) = source {
            let stop = retained
                .source_stop_command
                .as_deref()
                .map(|stop_id| load_command(&self.connection, stop_id))
                .transpose()?
                .flatten();
            let stop_settled = stop.as_ref().is_none_or(|stop| {
                matches!(
                    stop.state,
                    CommandState::Failed
                        | CommandState::Rejected
                        | CommandState::Cancelled
                        | CommandState::Applied
                )
            });
            let exit_proven = stop.as_ref().is_none_or(|stop| {
                stop.state != CommandState::Applied
                    || handoff::session_exit_blocker(&self.connection, &source)
                        .is_ok_and(|blocker| blocker.is_none())
            });
            if stop_settled && exit_proven {
                let tx = self
                    .connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(sql_error)?;
                release_switch_checkout_fences(
                    &tx,
                    id,
                    retained.source_checkout_key.as_deref(),
                    retained.target_checkout_key.as_deref(),
                    &source,
                    now,
                )?;
                commit_transaction(tx, &self.write_faulted)?;
            }
        }
        let mut value = self.switch_result(id)?;
        set_switch_error(&mut value, error, policy);
        value.state = CommandState::Rejected;
        value.next_action.clear();
        value.routing = error.routing.clone();
        let change = self.settle(
            id,
            CommandState::Rejected,
            Some(
                serde_json::to_value(value)
                    .map_err(|_| Error::new("internal", "cannot encode switch result"))?,
            ),
            now,
        )?;
        self.settle_automatic_handoff_attempt(id, CommandState::Rejected, now)?;
        Ok(change)
    }

    /// Latest exact provider switch fact for this attempt, including queueing.
    pub fn provider_switch_event(
        &self,
        agent: &str,
        generation: i64,
        task: Option<&str>,
    ) -> Result<Option<Event>> {
        self.ensure_committed_reads()?;
        let row = self
            .connection
            .query_row(
                "SELECT seq, ts, kind, agent, generation, source, native_id, data, subject, cause
             FROM events WHERE agent=?1 AND generation=?2 AND kind IN (?3,?4)
             AND (?5 IS NULL OR json_extract(data,'$.task_command')=?5)
             ORDER BY seq DESC LIMIT 1",
                params![
                    parse_agent_id(agent)?,
                    generation,
                    PROVIDER_SWITCH_QUEUED,
                    PROVIDER_SWITCH_SETTLED,
                    task
                ],
                event_record_from_row,
            )
            .optional()
            .map_err(sql_error)?;
        row.map(event_from_record).transpose()
    }
}

fn set_switch_error(value: &mut SwitchResponse, error: &Error, policy: &core::SecretPolicy) {
    value.error = Some(core::bound_chars(&policy.redact_text(&error.code), 128));
    value.error_reason = Some(core::bound_chars(&policy.redact_text(&error.message), 512));
}

fn replay_on(
    connection: &Connection,
    principal: &str,
    request: &SwitchRequest,
    digest: &[u8],
) -> Result<Option<SwitchResponse>> {
    if let Some((p, m, d)) = command_binding(connection, &request.id)? {
        if p != principal || m != METHOD || d != digest {
            return Err(Error::new("id_conflict", "switch id has another binding"));
        }
        let result = result_on(connection, &request.id)?;
        request.validate_response(&result)?;
        return Ok(Some(result));
    }
    if write_exists(connection, &request.id)? || digest_id_bound(connection, &request.id)? {
        return Err(Error::new("id_conflict", "switch id used by another write"));
    }
    Ok(None)
}

fn owner(principal: &str) -> Result<()> {
    if principal == "owner" {
        Ok(())
    } else {
        Err(Error::new("not_allowed", "only Owner may switch Agents"))
    }
}

fn switch_parent(connection: &Connection, id: &str) -> Result<Command> {
    let parent =
        load_command(connection, id)?.ok_or_else(|| Error::new("not_found", "switch missing"))?;
    if parent.method != METHOD || parent.verb != "switch" || parent.principal != "owner" {
        return Err(Error::new("bad_state", "not an Owner switch"));
    }
    Ok(parent)
}

fn stored_result(outcome: &Option<Value>) -> Result<SwitchResponse> {
    serde_json::from_value(
        outcome
            .clone()
            .ok_or_else(|| Error::new("source_integrity", "switch plan missing"))?,
    )
    .map_err(|_| Error::new("source_integrity", "switch plan malformed"))
}

fn component(
    connection: &Connection,
    parent: &str,
    id: &str,
    ordinal: i64,
) -> Result<Option<Command>> {
    let linked: bool = connection.query_row("SELECT EXISTS(SELECT 1 FROM commands WHERE id=?1 AND parent_command=?2 AND link_kind='switch' AND ordinal=?3)",params![id,parent,ordinal],|row|row.get(0)).map_err(sql_error)?;
    if linked {
        load_command(connection, id)
    } else {
        Ok(None)
    }
}

/// Check the exact unused successor again when its reserved stop dispatches.
/// An Owner may have admitted input to B after the refusal was committed.
pub(super) fn require_unused_switch_successor(
    tx: &Transaction<'_>,
    stop_id: &str,
    agent: &Agent,
) -> Result<()> {
    let switch_id: Option<String> = tx
        .query_row(
            "SELECT parent_command FROM commands WHERE id=?1 AND link_kind='switch' AND ordinal=2",
            [stop_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(sql_error)?;
    let Some(switch_id) = switch_id else {
        return Ok(());
    };
    let parent = switch_parent(tx, &switch_id)?;
    let receipt = stored_result(&parent.outcome)?;
    if parent.state != CommandState::Failed || receipt.disposal_command.as_deref() != Some(stop_id)
    {
        return Err(Error::new(
            "source_integrity",
            "switch successor stop does not match the refusal",
        ));
    }
    let launch = component(tx, &switch_id, &receipt.launch_command, 0)?
        .ok_or_else(|| Error::new("source_integrity", "switch launch is not retained"))?;
    let destination = launch_destination_tx(tx, &launch)?;
    if destination.as_ref().is_none_or(|destination| {
        destination.id != agent.id || destination.generation != agent.generation
    }) {
        return Err(Error::new(
            "stale_generation",
            "switch successor generation moved",
        ));
    }
    if switch_successor_has_input(tx, agent)? {
        return Err(Error::new(
            "bad_state",
            "Successor has a SEND or turn. Its session was left running.",
        ));
    }
    Ok(())
}

pub(super) fn refresh_disposal_parent_tx(
    tx: &Transaction<'_>,
    command_id: &str,
    state: CommandState,
    now: i64,
) -> Result<()> {
    tx.execute(
        "UPDATE commands SET outcome=json_set(outcome,'$.disposal_state',?2,'$.disposal_reason',
             (SELECT json_extract(outcome,'$.message') FROM commands WHERE id=?1)), updated_at=?3
         WHERE id=(SELECT parent_command FROM commands WHERE id=?1 AND link_kind='switch' AND ordinal=2 AND verb='stop')
           AND method='swarm.switch' AND state='failed' AND json_extract(outcome,'$.disposal_command')=?1",
        params![command_id, command_state_name(state), now],
    )
    .map_err(sql_error)?;
    if state == CommandState::Applied {
        let switch_id: Option<String> = tx
            .query_row(
                "SELECT parent_command FROM commands
                 WHERE id=?1 AND link_kind='switch' AND ordinal=2",
                [command_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)?;
        if let Some(switch_id) = switch_id {
            let parent = switch_parent(tx, &switch_id)?;
            let retained = decode_switch(parent.args)?;
            if let Some(source) = load_agent_by_num(tx, parse_agent_id(&retained.request.from)?)? {
                release_switch_checkout_fences(
                    tx,
                    &switch_id,
                    retained.source_checkout_key.as_deref(),
                    retained.target_checkout_key.as_deref(),
                    &source,
                    now,
                )?;
            }
        }
    }
    if matches!(
        state,
        CommandState::Applied
            | CommandState::Failed
            | CommandState::Rejected
            | CommandState::Cancelled
    ) {
        let switch_id: Option<String> = tx
            .query_row(
                "SELECT parent_command FROM commands
                 WHERE id=?1 AND link_kind='switch' AND ordinal=3",
                [command_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)?;
        if let Some(switch_id) = switch_id {
            let parent = switch_parent(tx, &switch_id)?;
            if parent.state == CommandState::Rejected {
                let retained = decode_switch(parent.args)?;
                if let Some(source) =
                    load_agent_by_num(tx, parse_agent_id(&retained.request.from)?)?
                {
                    let exit_proven = state != CommandState::Applied
                        || handoff::session_exit_blocker(tx, &source)?.is_none();
                    if exit_proven {
                        release_switch_checkout_fences(
                            tx,
                            &switch_id,
                            retained.source_checkout_key.as_deref(),
                            retained.target_checkout_key.as_deref(),
                            &source,
                            now,
                        )?;
                    }
                }
            }
        }
    }
    Ok(())
}

fn result_on(connection: &Connection, id: &str) -> Result<SwitchResponse> {
    let parent = switch_parent(connection, id)?;
    let mut result = stored_result(&parent.outcome)?;
    result.state = parent.state;
    if let Some(launch) = component(connection, id, &result.launch_command, 0)? {
        result.launch_state = Some(launch.state);
        result.created = launch.target.map(|agent| SwitchAgent {
            agent,
            generation: launch.generation.unwrap_or(0),
        });
    }
    if let Some(handoff) = component(connection, id, &result.handoff_command, 1)? {
        result.handoff = Some(
            serde_json::from_value(
                handoff
                    .outcome
                    .ok_or_else(|| Error::new("source_integrity", "handoff result missing"))?,
            )
            .map_err(|_| Error::new("source_integrity", "handoff result malformed"))?,
        );
    }
    if let Some(disposal_id) = &result.disposal_command {
        let disposal = component(connection, id, disposal_id, 2)?.ok_or_else(|| {
            Error::new("source_integrity", "switch successor stop is not retained")
        })?;
        if disposal.verb != "stop"
            || result.created.as_ref().is_none_or(|agent| {
                disposal.target.as_deref() != Some(&agent.agent)
                    || disposal.generation != Some(agent.generation)
            })
        {
            return Err(Error::new(
                "source_integrity",
                "switch successor stop binding changed",
            ));
        }
        result.disposal_state = Some(disposal.state);
    }
    result.next_action = switch_next_action(&result).into();
    Ok(result)
}

fn switch_next_action(result: &SwitchResponse) -> &'static str {
    match result.state {
        CommandState::Applied => "read_handoff_children",
        CommandState::Failed | CommandState::Rejected | CommandState::Cancelled => "",
        CommandState::Unknown if result.launch_state == Some(CommandState::Unknown) => {
            "resolve_launch_unknown_never_relaunch"
        }
        CommandState::Unknown => "inspect_retained_children_never_retry_launch",
        CommandState::Accepted => "preparing_launch",
        _ => "wait_for_switch",
    }
}

fn source_tx(
    tx: &Transaction<'_>,
    request: &SwitchRequest,
    limits: core::api::HierarchyLimits,
    policy: &core::SecretPolicy,
) -> Result<(Agent, Option<core::api::ProjectRegistrationView>)> {
    if request.commands.len() as i64 > limits.max_selector_targets {
        return Err(Error::new(
            "fanout_limit",
            "switch selection exceeds configured bound",
        ));
    }
    let source = load_agent_by_num(tx, parse_agent_id(&request.from)?)?
        .ok_or_else(|| Error::new("not_found", "source Agent missing"))?;
    if source.generation != request.from_generation {
        return Err(Error::new("stale_generation", "source generation moved"));
    }
    handoff::transfer_source_ready(tx, &source)?;
    project_transfer::require_no_intent(tx, &source.project)?;
    handoff::source_inputs(
        tx,
        &source,
        &request.task_command,
        &request.commands,
        None,
        policy,
    )?;
    goals::require_handoff_source_goal(tx, &source, policy)?;
    require_switch_children(tx, &source, limits)?;
    // Continue-in-project binds the target registration at reserve time, so
    // preparation resolves the target checkout and never the source repo.
    // A projectless source has no execution project for B: it must name one.
    // An Agent continued into a project keeps that execution project, so an
    // ordinary switch afterwards needs no target.
    let target = match request.launch.project.as_deref() {
        Some(project) => Some(projects::project_registration_tx(tx, project)?.ok_or_else(
            || {
                Error::new(
                    "not_found",
                    format!("switch target project {project} was not found"),
                )
            },
        )?),
        None if source.project.is_empty() => {
            return Err(Error::new(
                "invalid",
                "switch from a projectless chat needs a target project",
            ));
        }
        None => None,
    };
    if let Some(registration) = &target {
        project_transfer::require_no_intent(tx, &registration.id)?;
    }
    Ok((source, target))
}

fn require_switch_children(
    tx: &Transaction<'_>,
    source: &Agent,
    limits: core::api::HierarchyLimits,
) -> Result<()> {
    let source_num = parse_agent_id(&source.id)?;
    // B is a new root. Its controlled tree has the same size as this
    // source's controlled subtree, with B taking the place of A.
    let count_limit = limits
        .max_tree_agents
        .min(limits.max_selector_targets.saturating_add(1));
    let members = hierarchy::controlled_subtree_member_nums(tx, source_num, count_limit)?;
    if members.len() as i64 > count_limit {
        return Err(Error::new(
            "fanout_limit",
            format!(
                "Agent {} has too many controlled descendants to transfer",
                source.id
            ),
        ));
    }
    let mut direct = 0_i64;
    for id in members.into_iter().filter(|id| *id != source_num) {
        let child = load_agent_by_num(tx, id)?
            .ok_or_else(|| Error::new("source_integrity", "switch child disappeared"))?;
        if child.project_scope() != source.project_scope() {
            return Err(Error::new(
                "not_allowed",
                format!(
                    "controlled child {} is outside the source project scope",
                    child.id
                ),
            ));
        }
        if !(1..=limits.max_depth).contains(&child.depth.saturating_sub(source.depth)) {
            return Err(Error::new(
                "fanout_limit",
                format!(
                    "controlled child {} would exceed the successor depth limit",
                    child.id
                ),
            ));
        }
        direct += i64::from(child.parent.as_deref() == Some(&source.id));
    }
    if direct > limits.max_children_per_agent {
        return Err(Error::new(
            "fanout_limit",
            format!(
                "Agent {} has too many direct controlled children to transfer",
                source.id
            ),
        ));
    }
    Ok(())
}

fn launch_destination_tx(tx: &Connection, launch: &Command) -> Result<Option<Agent>> {
    let (Some(target), Some(1)) = (launch.target.as_deref(), launch.generation) else {
        return Ok(None);
    };
    Ok(load_agent_by_num(tx, parse_agent_id(target)?)?
        .filter(|agent| launch.verb == "launch" && agent.generation == 1 && !agent.observe_only))
}

fn switch_successor_needs_stop(tx: &Connection, agent: &Agent) -> Result<bool> {
    if !agent.is_terminal() {
        return Ok(true);
    }
    tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM processes WHERE owner=?1 AND state IN ('running','unknown'))",
        [format!("agent:{}#{}", agent.id, agent.generation)],
        |row| row.get(0),
    )
    .map_err(sql_error)
}

fn switch_successor_has_input(tx: &Connection, agent: &Agent) -> Result<bool> {
    tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM commands WHERE target=?1 AND generation=?2 AND verb='send')
          OR EXISTS(SELECT 1 FROM turns WHERE agent=?1 AND generation=?2)",
        params![parse_agent_id(&agent.id)?, agent.generation],
        |row| row.get(0),
    )
    .map_err(sql_error)
}

pub(super) fn require_switch_successor_accepting_input(
    tx: &Transaction<'_>,
    agent: i64,
    generation: i64,
) -> Result<()> {
    let disposing: bool = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM commands WHERE target=?1 AND generation=?2
           AND link_kind='switch' AND ordinal=2 AND verb='stop'
           AND state IN ('dispatched','unknown'))",
            params![agent, generation],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if disposing {
        return Err(Error::new(
            "busy",
            "Switch successor stop is already in flight.",
        ));
    }
    Ok(())
}

fn ready_destination_tx(tx: &Connection, launch: &Command) -> Result<Agent> {
    let destination = launch_destination_tx(tx, launch)?.ok_or_else(|| {
        Error::new(
            "stale_generation",
            "switch launch generation is unavailable",
        )
    })?;
    let agent = &destination.id;
    // Some native adapters omit command_id on session.ready. The first generation
    // of this newly allocated B has exactly one start command: the reserved launch.
    // Use that exact persistent binding; a contradictory supplied ID still refuses.
    let ready: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM events WHERE agent=?1 AND generation=1 AND kind='session.ready' AND source='stream' AND json_extract(subject,'$.state')='idle'
        AND (json_extract(data,'$.command_id') IS NULL OR json_extract(data,'$.command_id')=?2)
        AND (json_extract(cause,'$.command_id') IS NULL OR json_extract(cause,'$.command_id')=?2))
        AND NOT EXISTS(SELECT 1 FROM commands WHERE target=?1 AND generation=1 AND verb IN ('launch','resume') AND id<>?2)", params![parse_agent_id(agent)?,launch.id], |row|row.get(0)).map_err(sql_error)?;
    if launch.verb != "launch"
        || destination.generation != 1
        || launch.generation != Some(destination.generation)
        || !ready
        || destination.state != AgentState::Idle
        || launch
            .outcome
            .as_ref()
            .and_then(|value| value.get("resolution_source"))
            .is_some()
    {
        return Err(Error::new(
            "bad_state",
            "switch requires exact committed session readiness",
        ));
    }
    if switch_successor_has_input(tx, &destination)? {
        return Err(Error::new(
            "bad_state",
            "switch destination already has input",
        ));
    }
    Ok(destination)
}
