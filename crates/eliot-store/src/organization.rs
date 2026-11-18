//! Headless chat and work organization (step 4a): placement overlay, tags,
//! grouped projection and project continuation.
//!
//! Organization is placement, never authority. Attach/detach move a chat's
//! display placement between a retained project and the no-project bucket;
//! tags group Agents and chats. Execution project, scope, workspace and
//! history are never edited here. `chat.continue` starts a new authorized
//! project session and records a source reference; the source keeps its
//! history and usage attribution.

use super::*;
use eliot_core::api::{
    ArchivedFilter, ChatAttachRequest, ChatContinueRequest, ChatContinueResponse,
    ChatDetachRequest, ChatOrganizationMutationResponse, ChatOrganizationRequest,
    ChatOrganizationResponse, ChatTagRequest, MAX_ORG_MEMBERS_PER_GROUP, MAX_ORG_MEMBERS_TOTAL,
    OrgGroupView, OrgLinkView, OrgMemberView, OrgSourceRef, ProjectAssignment,
    ProjectManagerIdentity, SwitchAgent,
};
use std::collections::BTreeMap;

pub(super) const CHAT_ATTACH_METHOD: &str = "chat.attach";
pub(super) const CHAT_DETACH_METHOD: &str = "chat.detach";
pub(super) const CHAT_TAG_METHOD: &str = "chat.tag";
pub(super) const CHAT_CONTINUE_METHOD: &str = "chat.continue";

fn owner_only(principal: &str, what: &str) -> Result<()> {
    if principal == "owner" {
        Ok(())
    } else {
        Err(Error::new("not_allowed", format!("only Owner may {what}")))
    }
}

fn organization_revision(conn: &Connection) -> Result<i64> {
    conn.query_row("SELECT revision FROM organization_state", [], |row| {
        row.get(0)
    })
    .map_err(sql_error)
}

fn bump_organization_revision(tx: &Transaction<'_>) -> Result<i64> {
    db!(tx.execute("UPDATE organization_state SET revision = revision + 1", []));
    tx.query_row("SELECT revision FROM organization_state", [], |row| {
        row.get(0)
    })
    .map_err(sql_error)
}

fn organization_watermark(conn: &Connection) -> Result<i64> {
    conn.query_row(
        "SELECT MAX(COALESCE((SELECT MAX(seq) FROM events), 0),
                COALESCE((SELECT seq FROM sqlite_sequence WHERE name = 'events'), 0))",
        [],
        |row| row.get(0),
    )
    .map_err(sql_error)
}

fn decode_tags(raw: &str) -> Result<Vec<String>> {
    let tags: Vec<String> = serde_json::from_str(raw)
        .map_err(|_| Error::new("source_integrity", "stored tags invalid"))?;
    if tags.len() > eliot_core::api::MAX_ORG_TAGS {
        return Err(Error::new("source_integrity", "stored tags exceed bound"));
    }
    let mut seen = std::collections::HashSet::new();
    for tag in &tags {
        if tag.is_empty()
            || tag.len() > eliot_core::api::MAX_ORG_TAG_BYTES
            || tag.trim() != tag
            || tag.contains(char::is_control)
            || !seen.insert(tag)
        {
            return Err(Error::new("source_integrity", "stored tags invalid"));
        }
    }
    Ok(tags)
}

struct OrgRow {
    link: Option<String>,
    tags: Vec<String>,
}

fn load_org_rows(conn: &Connection) -> Result<BTreeMap<i64, OrgRow>> {
    let mut statement = conn
        .prepare("SELECT agent, project_link, tags FROM chat_organization")
        .map_err(sql_error)?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .map_err(sql_error)?;
    let mut map = BTreeMap::new();
    for row in rows {
        let (agent, link, tags) = row.map_err(sql_error)?;
        map.insert(
            agent,
            OrgRow {
                link,
                tags: decode_tags(&tags)?,
            },
        );
    }
    Ok(map)
}

fn load_continuations(conn: &Connection) -> Result<BTreeMap<i64, (i64, i64)>> {
    let mut statement = conn
        .prepare("SELECT session, source, source_generation FROM chat_continuations")
        .map_err(sql_error)?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })
        .map_err(sql_error)?;
    let mut map = BTreeMap::new();
    for row in rows {
        let (session, source, generation) = row.map_err(sql_error)?;
        map.insert(session, (source, generation));
    }
    Ok(map)
}

fn manager_view(
    conn: &Connection,
    assignment: &ProjectAssignment,
) -> Result<Option<eliot_core::api::OverviewManagerView>> {
    let Some(ProjectManagerIdentity::Local { agent, generation }) = assignment.manager.as_ref()
    else {
        return Ok(None);
    };
    let state = load_agent_by_num(conn, parse_agent_id(agent)?)?
        .filter(|row| {
            row.generation == *generation
                || (row.generation == 0
                    && *generation == 1
                    && matches!(
                        assignment.state,
                        eliot_core::api::ProjectAssignmentState::Starting
                    ))
        })
        .map_or(AgentState::Unknown, |row| row.state);
    Ok(Some(eliot_core::api::OverviewManagerView {
        agent: agent.clone(),
        generation: *generation,
        state,
        assignment_revision: assignment.revision,
        workspace: assignment.workspace.clone(),
    }))
}

struct ScannedMember {
    key: Option<String>,
    view: OrgMemberView,
}

enum LinkChange {
    Attach { project: String },
    Detach,
    Tag { tags: Vec<String> },
}

enum LinkRequest<'a> {
    Attach(&'a ChatAttachRequest),
    Detach(&'a ChatDetachRequest),
    Tag(&'a ChatTagRequest),
}

impl LinkRequest<'_> {
    fn id(&self) -> &str {
        match self {
            LinkRequest::Attach(request) => &request.id,
            LinkRequest::Detach(request) => &request.id,
            LinkRequest::Tag(request) => &request.id,
        }
    }

    fn agents(&self) -> &[String] {
        match self {
            LinkRequest::Attach(request) => &request.agents,
            LinkRequest::Detach(request) => &request.agents,
            LinkRequest::Tag(request) => &request.agents,
        }
    }

    fn change(&self) -> LinkChange {
        match self {
            LinkRequest::Attach(request) => LinkChange::Attach {
                project: request.project.clone(),
            },
            LinkRequest::Detach(_) => LinkChange::Detach,
            LinkRequest::Tag(request) => LinkChange::Tag {
                tags: request.tags.clone(),
            },
        }
    }

    fn encode(&self) -> Result<Value> {
        match self {
            LinkRequest::Attach(request) => serde_json::to_value(request),
            LinkRequest::Detach(request) => serde_json::to_value(request),
            LinkRequest::Tag(request) => serde_json::to_value(request),
        }
        .map_err(|_| Error::new("internal", "organization request could not be encoded"))
    }
}

impl Store {
    /// Grouped work projection: one group per project in the retained union
    /// (registrations, assignments, execution labels) plus the unplaced
    /// bucket. Groups follow project assignment, manager,
    /// repository/worktree and ancestry; chats additionally follow their
    /// placement link. Owner only. The caller gets a ready projection with
    /// the organization revision and readback, so no client repeats Git
    /// discovery or grouping.
    pub fn chat_organization(
        &self,
        principal: &str,
        request: &ChatOrganizationRequest,
        now: i64,
    ) -> Result<ChatOrganizationResponse> {
        request.validate()?;
        owner_only(principal, "read organization")?;
        self.ensure_committed_reads()?;
        let conn = &self.connection;
        let revision = organization_revision(conn)?;
        let watermark = organization_watermark(conn)?;
        let union: Vec<String> = conn
            .prepare(
                "SELECT project FROM (
                     SELECT id AS project FROM project_registrations
                     UNION SELECT project FROM project_assignments
                     UNION SELECT project FROM agents WHERE project <> ''
                 ) WHERE (?1 IS NULL OR project = ?1) ORDER BY project",
            )
            .map_err(sql_error)?
            .query_map(params![request.project.as_deref()], |row| row.get(0))
            .map_err(sql_error)?
            .collect::<rusqlite::Result<_>>()
            .map_err(sql_error)?;
        if let Some(project) = request.project.as_deref()
            && !union.iter().any(|entry| entry == project)
        {
            return Err(Error::new(
                "not_found",
                format!("project {project} was not found"),
            ));
        }
        let registered: BTreeMap<String, ()> = conn
            .prepare("SELECT id FROM project_registrations")
            .map_err(sql_error)?
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(sql_error)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(sql_error)?
            .into_iter()
            .map(|id| (id, ()))
            .collect();
        let org_rows = load_org_rows(conn)?;
        let continuations = load_continuations(conn)?;
        let mut statement = conn
            .prepare(
                "SELECT id, generation, state, harness, parent, root, project,
                        chat, archived, observe_only, workspace
                 FROM agents ORDER BY id",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, Option<i64>>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, bool>(7)?,
                    row.get::<_, bool>(8)?,
                    row.get::<_, bool>(9)?,
                    row.get::<_, Option<String>>(10)?,
                ))
            })
            .map_err(sql_error)?;
        // One ordered scan; rows stay in memory so totals are exact while
        // returned rows truncate per group and across the projection.
        // Roots are tracked beside the rows: a project with no manager but
        // root members reads as needing assignment, never auto-assigned.
        let mut scanned: Vec<ScannedMember> = Vec::new();
        let mut counts: BTreeMap<Option<String>, usize> = BTreeMap::new();
        let mut has_root: BTreeMap<Option<String>, bool> = BTreeMap::new();
        for row in rows {
            let (
                num,
                generation,
                state,
                harness,
                parent,
                root,
                project,
                chat,
                archived,
                observe_only,
                workspace,
            ) = row.map_err(sql_error)?;
            if match request.archived {
                ArchivedFilter::Exclude => archived,
                ArchivedFilter::Only => !archived,
                ArchivedFilter::Include => false,
            } {
                continue;
            }
            let org = org_rows.get(&num);
            let link = org.and_then(|entry| entry.link.clone());
            let placed_project = link
                .clone()
                .filter(|target| registered.contains_key(target));
            let key = if chat {
                placed_project.or_else(|| {
                    if registered.contains_key(&project) {
                        Some(project.clone())
                    } else {
                        None
                    }
                })
            } else if registered.contains_key(&project) {
                Some(project.clone())
            } else {
                None
            };
            if request
                .project
                .as_deref()
                .is_some_and(|filter| key.as_deref() != Some(filter))
            {
                continue;
            }
            *counts.entry(key.clone()).or_default() += 1;
            if parent.is_none() && !observe_only && !chat {
                has_root.insert(key.clone(), true);
            }
            scanned.push(ScannedMember {
                key,
                view: OrgMemberView {
                    agent: format_agent_id(num),
                    generation,
                    state: parse_enum(&state, "Agent state")?,
                    harness,
                    parent: parent.map(format_agent_id),
                    root: format_agent_id(root),
                    project,
                    chat,
                    archived,
                    project_link: link.clone(),
                    tags: org.map(|entry| entry.tags.clone()).unwrap_or_default(),
                    placed: chat && link.is_some(),
                    continued_from: continuations.get(&num).map(|(source, generation)| {
                        OrgSourceRef {
                            agent: format_agent_id(*source),
                            generation: *generation,
                        }
                    }),
                    workspace,
                },
            });
        }
        let grand_total: usize = scanned.len();
        // Fill in response order (projects first, unplaced last) until the
        // projection cap; totals stay exact and truncation stays explicit.
        let mut budget = MAX_ORG_MEMBERS_TOTAL;
        let mut kept = 0_usize;
        let mut response = ChatOrganizationResponse {
            revision,
            event_watermark: watermark,
            as_of: now,
            groups: Vec::new(),
            member_total: grand_total,
            truncated: false,
        };
        let mut order: Vec<Option<String>> =
            union.iter().map(|project| Some(project.clone())).collect();
        if request.project.is_none() {
            order.push(None);
        }
        for key in order {
            let full = counts.get(&key).copied().unwrap_or(0);
            if full == 0 {
                // Empty retained projects still read back their team detail.
                if key.is_some() {
                    response.groups.push(self.organization_group(
                        conn,
                        key,
                        Vec::new(),
                        full,
                        false,
                        false,
                    )?);
                }
                continue;
            }
            let take = full.min(MAX_ORG_MEMBERS_PER_GROUP).min(budget);
            budget -= take;
            kept += take;
            let mut members = Vec::with_capacity(take);
            for member in scanned.iter().filter(|member| member.key == key).take(take) {
                members.push(member.view.clone());
            }
            let needs_assignment = has_root.get(&key).copied().unwrap_or(false);
            response.groups.push(self.organization_group(
                conn,
                key,
                members,
                full,
                full > take,
                needs_assignment,
            )?);
        }
        response.truncated = grand_total > kept;
        Ok(response)
    }

    fn organization_group(
        &self,
        conn: &Connection,
        project: Option<String>,
        members: Vec<OrgMemberView>,
        member_total: usize,
        members_truncated: bool,
        has_root: bool,
    ) -> Result<OrgGroupView> {
        let (registration, assignment, manager, needs_assignment) = match project.as_deref() {
            Some(project) => {
                let registration = projects::project_registration_tx(conn, project)?;
                if registration.is_none() {
                    // Execution labels and assignments without a retained
                    // registration group by label but carry no team detail.
                    (None, None, None, false)
                } else {
                    let assignment = project_assignment::read_assignment(conn, project)?;
                    let manager = manager_view(conn, &assignment)?;
                    let needs_assignment = assignment.manager.is_none() && has_root;
                    (registration, Some(assignment), manager, needs_assignment)
                }
            }
            None => (None, None, None, false),
        };
        Ok(OrgGroupView {
            project,
            registration,
            assignment,
            needs_assignment,
            manager,
            members,
            member_total,
            members_truncated,
        })
    }
    /// Shared attach/detach/tag transaction: validate everything, then
    /// apply link rows and the global revision atomically. Placement only:
    /// execution project, scope, workspace and history are untouched.
    /// Attach and detach place chats; tags group any Agent or chat.
    fn mutate_links(
        &mut self,
        principal: &str,
        method: &str,
        request: LinkRequest<'_>,
        now: i64,
    ) -> Result<Change<ChatOrganizationMutationResponse>> {
        let id = request.id();
        let agents = request.agents();
        let change = request.change();
        let payload = request.encode()?;
        let digest = canonical::digest(&payload)?;
        if let Some((old_principal, old_method, old_digest, result)) =
            write_binding(&self.connection, id)?
        {
            if old_principal != principal || old_method != method || old_digest != digest {
                return Err(Error::new(
                    "id_conflict",
                    "organization operation id is already bound",
                ));
            }
            let value: ChatOrganizationMutationResponse = serde_json::from_str(&result)
                .map_err(|_| Error::new("source_integrity", "organization result malformed"))?;
            return Ok(Change {
                value,
                events: Vec::new(),
            });
        }
        if write_exists(&self.connection, id)? || digest_id_bound(&self.connection, id)? {
            return Err(Error::new(
                "id_conflict",
                "organization operation id is already in use",
            ));
        }
        validate_ulid(id)?;
        validate_id_age(id, now)?;
        self.ensure_writer_healthy()?;
        let result = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let mut nums = Vec::with_capacity(agents.len());
            for agent in agents {
                let num = parse_agent_id(agent)?;
                let row = load_agent_by_num(&tx, num)?.ok_or_else(|| {
                    Error::new("not_found", format!("Agent {agent} was not found"))
                })?;
                match &change {
                    LinkChange::Attach { .. } | LinkChange::Detach => {
                        if !row.chat {
                            return Err(Error::new(
                                "not_allowed",
                                "organization placement moves chats only",
                            ));
                        }
                    }
                    LinkChange::Tag { .. } => {}
                }
                nums.push((agent.clone(), num));
            }
            if let LinkChange::Attach { project } = &change
                && projects::project_registration_tx(&tx, project)?.is_none()
            {
                return Err(Error::new(
                    "not_found",
                    format!("project {project} was not found"),
                ));
            }
            let revision = bump_organization_revision(&tx)?;
            let mut links = Vec::with_capacity(nums.len());
            for (agent, num) in nums {
                let current: Option<(Option<String>, String)> = tx
                    .query_row(
                        "SELECT project_link, tags FROM chat_organization WHERE agent = ?1",
                        [num],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .optional()
                    .map_err(sql_error)?;
                let (link, tags) = match &change {
                    LinkChange::Attach { project } => (
                        Some(project.clone()),
                        current
                            .map(|(_, tags)| tags)
                            .unwrap_or_else(|| "[]".to_owned()),
                    ),
                    LinkChange::Detach => (
                        None,
                        current
                            .map(|(_, tags)| tags)
                            .unwrap_or_else(|| "[]".to_owned()),
                    ),
                    LinkChange::Tag { tags } => {
                        let encoded = encode_json(tags)?;
                        (current.and_then(|(link, _)| link), encoded)
                    }
                };
                db!(tx.execute(
                    "INSERT INTO chat_organization(agent, project_link, tags, revision, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5)
                     ON CONFLICT(agent) DO UPDATE SET project_link = excluded.project_link,
                         tags = excluded.tags, revision = excluded.revision, updated_at = excluded.updated_at",
                    params![num, link, tags, revision, now],
                ));
                links.push(OrgLinkView {
                    agent,
                    project_link: link,
                    tags: decode_tags(&tags)?,
                    revision,
                });
            }
            let value = ChatOrganizationMutationResponse {
                id: id.to_owned(),
                revision,
                links,
            };
            coordination::retain_write_tx(
                &tx,
                id,
                principal,
                method,
                &digest,
                &serde_json::to_value(&value)
                    .map_err(|_| Error::new("internal", "cannot encode organization result"))?,
                now,
            )?;
            commit_transaction(tx, &self.write_faulted)?;
            Ok(Change {
                value,
                events: Vec::new(),
            })
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    /// `chat.attach`: place chats under a retained project. Placement only.
    pub fn attach_chats(
        &mut self,
        principal: &str,
        request: &ChatAttachRequest,
        now: i64,
    ) -> Result<Change<ChatOrganizationMutationResponse>> {
        request.validate()?;
        owner_only(principal, "attach chats")?;
        self.mutate_links(
            principal,
            CHAT_ATTACH_METHOD,
            LinkRequest::Attach(request),
            now,
        )
    }

    /// `chat.detach`: return chats to the no-project bucket. Placement only.
    pub fn detach_chats(
        &mut self,
        principal: &str,
        request: &ChatDetachRequest,
        now: i64,
    ) -> Result<Change<ChatOrganizationMutationResponse>> {
        request.validate()?;
        owner_only(principal, "detach chats")?;
        self.mutate_links(
            principal,
            CHAT_DETACH_METHOD,
            LinkRequest::Detach(request),
            now,
        )
    }

    /// `chat.tag`: replace the tag groups of Agents or chats.
    pub fn tag_agents(
        &mut self,
        principal: &str,
        request: &ChatTagRequest,
        now: i64,
    ) -> Result<Change<ChatOrganizationMutationResponse>> {
        request.validate()?;
        owner_only(principal, "tag Agents")?;
        self.mutate_links(principal, CHAT_TAG_METHOD, LinkRequest::Tag(request), now)
    }
    /// Router preflight: retained retries during drain, before new admission.
    pub fn replay_chat_continue(
        &self,
        principal: &str,
        request: &ChatContinueRequest,
    ) -> Result<Option<ChatContinueResponse>> {
        self.ensure_committed_reads()?;
        request.validate()?;
        owner_only(principal, "continue chats")?;
        let digest = canonical::digest(
            &serde_json::to_value(request)
                .map_err(|_| Error::new("internal", "cannot encode continue"))?,
        )?;
        continue_replay_on(&self.connection, principal, request, &digest)
    }

    /// Reserve one project continuation: the no-project to project crossing
    /// is explicit in the request, and the parent command carries the
    /// reserved launch and brief identities before any external work.
    pub fn reserve_chat_continue(
        &mut self,
        principal: &str,
        request: &ChatContinueRequest,
        policy: &core::SecretPolicy,
        now: i64,
    ) -> Result<Change<ChatContinueResponse>> {
        if let Some(value) = self.replay_chat_continue(principal, request)? {
            return Ok(Change {
                value,
                events: Vec::new(),
            });
        }
        self.ensure_writer_healthy()?;
        let result = (|| {
            let args = serde_json::to_value(request)
                .map_err(|_| Error::new("internal", "cannot encode continue"))?;
            let digest = canonical::digest(&args)?;
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            if let Some(value) = continue_replay_on(&tx, principal, request, &digest)? {
                commit_transaction(tx, &self.write_faulted)?;
                return Ok(Change {
                    value,
                    events: Vec::new(),
                });
            }
            validate_ulid(&request.id)?;
            validate_id_age(&request.id, now)?;
            if policy.contains_value(&args) {
                return Err(Error::new(
                    "secret_in_payload",
                    "continue contains a known secret",
                ));
            }
            if encode_json(&args)?.len() > MAX_EVENT_BYTES {
                return Err(Error::new(
                    "frame_too_large",
                    "continue exceeds command budget",
                ));
            }
            let source_num = parse_agent_id(&request.chat)?;
            let source = load_agent_by_num(&tx, source_num)?
                .ok_or_else(|| Error::new("not_found", "source conversation was not found"))?;
            let registration = projects::project_registration_tx(&tx, &request.project)?
                .ok_or_else(|| {
                    Error::new(
                        "not_found",
                        "continue project is not a retained registration",
                    )
                })?;
            if !registration.actions.iter().any(|action| action == "launch") {
                return Err(Error::new(
                    "not_allowed",
                    "project registration does not approve new sessions",
                ));
            }
            if let Some(profile) = request.launch.profile.as_deref()
                && !registration
                    .profiles
                    .iter()
                    .any(|approved| approved == profile)
            {
                return Err(Error::new(
                    "not_allowed",
                    "project registration does not approve the launch profile",
                ));
            }
            project_transfer::require_no_intent(&tx, &request.project)?;
            let workspace = request
                .launch
                .workspace
                .clone()
                .unwrap_or_else(|| registration.checkout.clone());
            let launch_command = Ulid::new().to_string();
            let value = ChatContinueResponse {
                id: request.id.clone(),
                state: CommandState::Accepted,
                launch_command: launch_command.clone(),
                brief_command: format!("{launch_command}/send"),
                created: None,
                source: OrgSourceRef {
                    agent: source.id.clone(),
                    generation: source.generation,
                },
                project: request.project.clone(),
                workspace,
                registration_revision: registration.revision,
                error: None,
                next_action: "preparing_launch".to_owned(),
            };
            let mut parent = SubmitCommand::new(&request.id, "continue");
            parent.args = args;
            let outcome = serde_json::to_value(&value)
                .map_err(|_| Error::new("internal", "cannot encode continue result"))?;
            let event = admit_command(
                &tx,
                &CommandInsert {
                    method: CHAT_CONTINUE_METHOD,
                    state: CommandState::Accepted,
                    outcome: Some(&outcome),
                    ..CommandInsert::targetless_admitted(principal, &parent, &digest, now)
                },
                json!({"command_id": request.id, "verb": "continue"}),
            )?;
            commit_transaction(tx, &self.write_faulted)?;
            Ok(Change {
                value,
                events: vec![event],
            })
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    /// Claim one continuation once. Retries are readback, not another launch.
    pub fn start_chat_continue(
        &mut self,
        id: &str,
        now: i64,
    ) -> Result<Change<Option<(ChatContinueRequest, ChatContinueResponse)>>> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let command = continue_parent(&tx, id)?;
            if command.state != CommandState::Accepted {
                return Ok(Change {
                    value: None,
                    events: Vec::new(),
                });
            }
            let request: ChatContinueRequest = serde_json::from_value(command.args)
                .map_err(|_| Error::new("source_integrity", "continue request malformed"))?;
            let value = continue_result_on(&tx, id)?;
            update_command_state(
                &tx,
                id,
                CommandState::Dispatched,
                command.outcome.as_ref(),
                None,
                now,
            )?;
            let event = insert_daemon_command_event(
                &tx,
                now,
                "command.dispatched",
                None,
                None,
                id,
                json!({"component": "continue_orchestration"}),
            )?;
            commit_transaction(tx, &self.write_faulted)?;
            Ok(Change {
                value: Some((request, value)),
                events: vec![event],
            })
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    /// Admit the reserved launch in the same transaction as launch admission,
    /// capacity and workspace reservation. The first input is the source
    /// reference brief: pointers to the retained source, never a transcript.
    pub fn admit_continue_launch(
        &mut self,
        id: &str,
        request: SubmitCommand,
        preparation: Option<WorkspacePreparationInput>,
        now: i64,
    ) -> Result<Admission> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let parent = continue_parent(&tx, id)?;
            let plan = continue_result_on(&tx, id)?;
            if parent.state != CommandState::Dispatched
                || request.id != plan.launch_command
                || request.verb != "launch"
                || request.target.is_some()
                || request
                    .args
                    .get("text")
                    .is_none_or(|text| text.as_str().is_none_or(str::is_empty))
                || request.args.get("parent").is_some()
            {
                return Err(Error::new(
                    "bad_state",
                    "continue launch binding or phase differs",
                ));
            }
            let source: ChatContinueRequest = serde_json::from_value(parent.args)
                .map_err(|_| Error::new("source_integrity", "continue request malformed"))?;
            if request.args.get("project").and_then(Value::as_str) != Some(source.project.as_str())
                || request
                    .args
                    .get("registered_project")
                    .and_then(Value::as_str)
                    != Some(source.project.as_str())
            {
                return Err(Error::new(
                    "not_allowed",
                    "continue launch must join the reserved project",
                ));
            }
            let registration = projects::project_registration_tx(&tx, &source.project)?
                .ok_or_else(|| Error::new("not_found", "continue project was removed"))?;
            if registration.revision != plan.registration_revision {
                return Err(Error::new(
                    "stale_generation",
                    "continue registration changed during preparation",
                ));
            }
            if load_command(&tx, &request.id)?.is_some()
                && continue_component(&tx, id, &request.id, 0)?.is_none()
            {
                return Err(Error::new(
                    "id_conflict",
                    "reserved launch id occupied by another command",
                ));
            }
            let admission = admission::submit_tx(
                &tx,
                "owner",
                request,
                preparation,
                now,
                false,
                self.hierarchy_limits,
                self.capacity_policy.as_deref(),
            )?;
            db!(tx.execute(
                "UPDATE commands SET parent_command = ?1, link_kind = 'continue', ordinal = 0 WHERE id = ?2",
                params![id, plan.launch_command],
            ));
            commit_transaction(tx, &self.write_faulted)?;
            Ok(admission)
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    /// Record the source reference beside the new session and settle the
    /// parent once its launch applied. The source keeps its history and
    /// usage attribution; only pointers are retained here.
    pub fn finish_chat_continue(
        &mut self,
        id: &str,
        now: i64,
    ) -> Result<Change<ChatContinueResponse>> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let parent = continue_parent(&tx, id)?;
            if parent.state != CommandState::Dispatched {
                let value = continue_result_on(&tx, id)?;
                return Ok(Change {
                    value,
                    events: Vec::new(),
                });
            }
            let plan = continue_result_on(&tx, id)?;
            let launch = continue_component(&tx, id, &plan.launch_command, 0)?
                .ok_or_else(|| Error::new("bad_state", "continue launch not admitted"))?;
            let mut events = Vec::new();
            let state: CommandState;
            let mut error = None;
            let mut created = None;
            if launch.state == CommandState::Applied {
                let target = launch.target.clone().ok_or_else(|| {
                    Error::new("source_integrity", "continue launch has no session")
                })?;
                let session = parse_agent_id(&target)?;
                let agent = load_agent_by_num(&tx, session)?.ok_or_else(|| {
                    Error::new("source_integrity", "continue session disappeared")
                })?;
                let source = parse_agent_id(&plan.source.agent)?;
                db!(tx.execute(
                    "INSERT INTO chat_continuations(session, source, source_generation, brief_command, created_at)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![session, source, plan.source.generation, plan.brief_command, now],
                ));
                created = Some(SwitchAgent {
                    agent: agent.id,
                    generation: agent.generation,
                });
                state = CommandState::Applied;
            } else {
                state = if matches!(
                    launch.state,
                    CommandState::Accepted
                        | CommandState::Queued
                        | CommandState::Dispatched
                        | CommandState::Unknown
                ) {
                    CommandState::Unknown
                } else {
                    CommandState::Failed
                };
                error = Some(Error::new(
                    "launch_not_ready",
                    "launch has no proven readiness",
                ));
            }
            if error.as_ref().is_some_and(|error| error.code == "internal") {
                return Err(error.unwrap());
            }
            let mut value = continue_result_on(&tx, id)?;
            value.error = error.map(|error| error.code);
            value.state = state;
            value.created = created;
            let outcome = serde_json::to_value(&value)
                .map_err(|_| Error::new("internal", "cannot encode continue result"))?;
            update_command_state(&tx, id, state, Some(&outcome), None, now)?;
            events.push(insert_daemon_command_event(
                &tx,
                now,
                &format!("command.{}", command_state_name(state)),
                None,
                None,
                id,
                json!({"launch_command": plan.launch_command, "brief_command": plan.brief_command}),
            )?);
            let value = continue_result_on(&tx, id)?;
            commit_transaction(tx, &self.write_faulted)?;
            Ok(Change { value, events })
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn fail_chat_continue_preparation(
        &mut self,
        id: &str,
        code: &str,
        now: i64,
    ) -> Result<Change<Command>> {
        let mut value = self.continue_result(id)?;
        value.error = Some(code.to_owned());
        self.settle(
            id,
            CommandState::Rejected,
            Some(
                serde_json::to_value(value)
                    .map_err(|_| Error::new("internal", "cannot encode continue result"))?,
            ),
            now,
        )
    }

    pub fn continue_result(&self, id: &str) -> Result<ChatContinueResponse> {
        self.ensure_committed_reads()?;
        continue_result_on(&self.connection, id)
    }

    pub fn continue_launch_ready(&self, id: &str) -> Result<bool> {
        self.ensure_committed_reads()?;
        let value = continue_result_on(&self.connection, id)?;
        let Some(launch) = load_command(&self.connection, &value.launch_command)? else {
            return Ok(false);
        };
        Ok(launch.state == CommandState::Applied)
    }
}

fn continue_replay_on(
    conn: &Connection,
    principal: &str,
    request: &ChatContinueRequest,
    digest: &[u8],
) -> Result<Option<ChatContinueResponse>> {
    if let Some((old_principal, method, old_digest)) = command_binding(conn, &request.id)? {
        if old_principal != principal || method != CHAT_CONTINUE_METHOD || old_digest != digest {
            return Err(Error::new("id_conflict", "continue id has another binding"));
        }
        let value = continue_result_on(conn, &request.id)?;
        request.validate_response(&value)?;
        return Ok(Some(value));
    }
    if write_exists(conn, &request.id)? || digest_id_bound(conn, &request.id)? {
        return Err(Error::new(
            "id_conflict",
            "continue id is used by another write",
        ));
    }
    Ok(None)
}

fn continue_parent(conn: &Connection, id: &str) -> Result<Command> {
    let parent =
        load_command(conn, id)?.ok_or_else(|| Error::new("not_found", "continue missing"))?;
    if parent.method != CHAT_CONTINUE_METHOD
        || parent.verb != "continue"
        || parent.principal != "owner"
    {
        return Err(Error::new("bad_state", "not an Owner continue"));
    }
    Ok(parent)
}

fn continue_result_on(conn: &Connection, id: &str) -> Result<ChatContinueResponse> {
    let outcome = load_command(conn, id)?
        .ok_or_else(|| Error::new("not_found", "continue missing"))?
        .outcome
        .ok_or_else(|| Error::new("source_integrity", "continue result missing"))?;
    serde_json::from_value(outcome)
        .map_err(|_| Error::new("source_integrity", "continue result malformed"))
}

fn continue_component(
    conn: &Connection,
    parent: &str,
    id: &str,
    ordinal: i64,
) -> Result<Option<Command>> {
    let linked: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM commands WHERE id = ?1 AND parent_command = ?2 AND link_kind = 'continue' AND ordinal = ?3)",
            params![id, parent, ordinal],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if linked {
        load_command(conn, id)
    } else {
        Ok(None)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::path::Path;

    fn now() -> i64 {
        i64::try_from(Ulid::new().timestamp_ms()).unwrap()
    }

    fn register(store: &mut Store, id: &str, checkout: &str) {
        store
            .upsert_project_registration(
                "owner",
                &ProjectRegistrationInput {
                    id: id.into(),
                    checkout: checkout.into(),
                    repo_common_dir: format!("{checkout}/.git"),
                    git: None,
                    profiles: vec!["approved".into()],
                    actions: vec!["launch".into()],
                    interpreters: BTreeMap::new(),
                    run_env: Vec::new(),
                },
                now(),
            )
            .unwrap();
    }

    fn launch(store: &mut Store, id: &str, project: &str, workspace: &str) -> String {
        let mut request = SubmitCommand::new(id.to_owned(), "launch");
        request.args = json!({
            "project": project,
            "workspace": workspace,
            "harness": "codex",
            "profile": "approved",
            "profile_snapshot": {"control": "tree"},
        });
        store
            .submit("owner", request, now())
            .unwrap()
            .command
            .target
            .unwrap()
    }

    fn launch_chat(store: &mut Store, id: &str, workspace: &str) -> String {
        let mut request = SubmitCommand::new(id.to_owned(), "launch");
        request.args = json!({
            "chat": true,
            "workspace": workspace,
            "harness": "codex",
            "profile": "approved",
            "profile_snapshot": {"control": "self"},
        });
        store
            .submit("owner", request, now())
            .unwrap()
            .command
            .target
            .unwrap()
    }

    fn attach_request(id: &str, agents: &[&str], project: &str) -> ChatAttachRequest {
        ChatAttachRequest {
            id: id.to_owned(),
            agents: agents.iter().map(|agent| agent.to_string()).collect(),
            project: project.to_owned(),
        }
    }

    fn continue_request(id: &str, chat: &str, project: &str) -> ChatContinueRequest {
        ChatContinueRequest {
            id: id.to_owned(),
            chat: chat.to_owned(),
            project: project.to_owned(),
            launch: serde_json::from_value(json!({"profile": "approved"})).unwrap(),
            text: None,
        }
    }

    #[test]
    fn attach_detach_round_trip_with_revision_and_replay() {
        let mut store = Store::open(Path::new(":memory:")).unwrap();
        register(&mut store, "alpha", "/managed/alpha");
        let chat = launch_chat(&mut store, &Ulid::new().to_string(), "/tmp/wt-k175-chat");
        let agent = launch(
            &mut store,
            &Ulid::new().to_string(),
            "alpha",
            "/managed/alpha",
        );

        // Placement moves chats only: a non-chat Agent is refused.
        let bad = attach_request(&Ulid::new().to_string(), &[&agent], "alpha");
        assert_eq!(
            store.attach_chats("owner", &bad, now()).unwrap_err().code,
            "not_allowed"
        );
        // Unknown projects and Agents are refused before any write.
        let bad = attach_request(&Ulid::new().to_string(), &[&chat], "missing");
        assert_eq!(
            store.attach_chats("owner", &bad, now()).unwrap_err().code,
            "not_found"
        );
        let bad = attach_request(&Ulid::new().to_string(), &["a999"], "alpha");
        assert_eq!(
            store.attach_chats("owner", &bad, now()).unwrap_err().code,
            "not_found"
        );
        // Non-owners cannot organize.
        let bad = attach_request(&Ulid::new().to_string(), &[&chat], "alpha");
        assert_eq!(
            store
                .attach_chats("agent:a1#1", &bad, now())
                .unwrap_err()
                .code,
            "not_allowed"
        );

        let first = attach_request(&Ulid::new().to_string(), &[&chat], "alpha");
        let attached = store.attach_chats("owner", &first, now()).unwrap().value;
        first.validate_response(&attached).unwrap();
        assert_eq!(attached.revision, 1);
        assert_eq!(attached.links[0].revision, 1);
        // Same id and payload replays instead of bumping the revision.
        let replayed = store.attach_chats("owner", &first, now()).unwrap().value;
        assert_eq!(replayed, attached);
        assert_eq!(organization_revision(&store.connection).unwrap(), 1);

        let detach = ChatDetachRequest {
            id: Ulid::new().to_string(),
            agents: vec![chat.clone()],
        };
        let detached = store.detach_chats("owner", &detach, now()).unwrap().value;
        detach.validate_response(&detached).unwrap();
        assert_eq!(detached.revision, 2);

        // Execution facts are untouched by placement.
        let row = load_agent_by_num(&store.connection, parse_agent_id(&chat).unwrap())
            .unwrap()
            .unwrap();
        assert!(row.project.is_empty());

        // The grouped projection reads the placement back.
        let projection = store
            .chat_organization(
                "owner",
                &ChatOrganizationRequest {
                    project: None,
                    archived: ArchivedFilter::Exclude,
                },
                now(),
            )
            .unwrap();
        assert_eq!(projection.revision, 2);
        assert!(projection.event_watermark >= 0);
        let unplaced = projection
            .groups
            .iter()
            .find(|group| group.project.is_none())
            .expect("unplaced bucket");
        assert!(unplaced.members.iter().any(|member| member.agent == chat));
    }

    #[test]
    fn tag_groups_any_agent_and_groups_follow_execution() {
        let mut store = Store::open(Path::new(":memory:")).unwrap();
        register(&mut store, "alpha", "/managed/alpha");
        register(&mut store, "beta", "/managed/beta");
        let root = launch(
            &mut store,
            &Ulid::new().to_string(),
            "alpha",
            "/managed/alpha",
        );
        let chat = launch_chat(&mut store, &Ulid::new().to_string(), "/tmp/wt-k175-chat-b");
        // Tags apply to chats and Agents alike.
        let tag = ChatTagRequest {
            id: Ulid::new().to_string(),
            agents: vec![root.clone(), chat.clone()],
            tags: vec!["inbox".to_owned(), "k175".to_owned()],
        };
        let tagged = store.tag_agents("owner", &tag, now()).unwrap().value;
        tag.validate_response(&tagged).unwrap();

        let projection = store
            .chat_organization(
                "owner",
                &ChatOrganizationRequest {
                    project: None,
                    archived: ArchivedFilter::Exclude,
                },
                now(),
            )
            .unwrap();
        // Projects sort first, the unplaced bucket sorts last.
        let names: Vec<Option<&str>> = projection
            .groups
            .iter()
            .map(|group| group.project.as_deref())
            .collect();
        assert_eq!(names, vec![Some("alpha"), Some("beta"), None]);
        let alpha = &projection.groups[0];
        assert_eq!(alpha.registration.as_ref().unwrap().id, "alpha");
        assert!(alpha.assignment.as_ref().unwrap().manager.is_none());
        // A root member with no manager reads as needing assignment.
        assert!(alpha.needs_assignment);
        assert!(
            alpha
                .members
                .iter()
                .any(|member| member.agent == root && member.tags == vec!["inbox", "k175"])
        );
        let unplaced = &projection.groups[2];
        assert!(
            unplaced
                .members
                .iter()
                .any(|member| member.agent == chat && member.tags == vec!["inbox", "k175"])
        );
    }

    #[test]
    fn attached_chats_read_under_their_project() {
        let mut store = Store::open(Path::new(":memory:")).unwrap();
        register(&mut store, "alpha", "/managed/alpha");
        let chat = launch_chat(&mut store, &Ulid::new().to_string(), "/tmp/wt-k175-chat-c");
        let first = attach_request(&Ulid::new().to_string(), &[&chat], "alpha");
        store.attach_chats("owner", &first, now()).unwrap();
        let projection = store
            .chat_organization(
                "owner",
                &ChatOrganizationRequest {
                    project: Some("alpha".to_owned()),
                    archived: ArchivedFilter::Exclude,
                },
                now(),
            )
            .unwrap();
        assert_eq!(projection.groups.len(), 1);
        let member = projection.groups[0]
            .members
            .iter()
            .find(|member| member.agent == chat)
            .expect("placed chat");
        assert!(member.placed);
        assert_eq!(member.project_link.as_deref(), Some("alpha"));
    }

    #[test]
    fn continue_reserve_names_project_and_source_without_touching_either() {
        let mut store = Store::open(Path::new(":memory:")).unwrap();
        register(&mut store, "alpha", "/managed/alpha");
        let chat = launch_chat(&mut store, &Ulid::new().to_string(), "/tmp/wt-k175-chat-d");
        let before = store.agent_count().unwrap();
        // Unknown registrations and profiles are refused before reserving.
        let mut missing = continue_request(&Ulid::new().to_string(), &chat, "missing");
        assert_eq!(
            store
                .reserve_chat_continue("owner", &missing, &core::SecretPolicy::default(), now())
                .unwrap_err()
                .code,
            "not_found"
        );
        missing.project = "alpha".to_owned();
        missing.launch = serde_json::from_value(json!({"profile": "other"})).unwrap();
        assert_eq!(
            store
                .reserve_chat_continue("owner", &missing, &core::SecretPolicy::default(), now())
                .unwrap_err()
                .code,
            "not_allowed"
        );
        let request = continue_request(&Ulid::new().to_string(), &chat, "alpha");
        let reserved = store
            .reserve_chat_continue("owner", &request, &core::SecretPolicy::default(), now())
            .unwrap()
            .value;
        request.validate_response(&reserved).unwrap();
        assert_eq!(reserved.source.agent, chat);
        assert_eq!(reserved.project, "alpha");
        assert_eq!(reserved.workspace, "/managed/alpha");
        assert_eq!(reserved.registration_revision, 1);
        assert_eq!(reserved.state, CommandState::Accepted);
        // Same id and payload replays the reservation; a different payload conflicts.
        let replayed = store
            .reserve_chat_continue("owner", &request, &core::SecretPolicy::default(), now())
            .unwrap()
            .value;
        assert_eq!(replayed.launch_command, reserved.launch_command);
        let mut conflict = request.clone();
        conflict.text = Some("different".to_owned());
        assert_eq!(
            store
                .reserve_chat_continue("owner", &conflict, &core::SecretPolicy::default(), now())
                .unwrap_err()
                .code,
            "id_conflict"
        );
        // Reservation created no session: the source is untouched.
        assert_eq!(store.agent_count().unwrap(), before);
        let source = load_agent_by_num(&store.connection, parse_agent_id(&chat).unwrap())
            .unwrap()
            .unwrap();
        assert!(source.project.is_empty());
    }
}
