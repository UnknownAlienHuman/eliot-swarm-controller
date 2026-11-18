//! Headless chat and work organization (step 4a): placement overlay, tags,
//! grouped projection and project continuation.
//!
//! Organization is placement, never authority. Attach/detach move a chat's
//! display placement between a retained project and the no-project bucket;
//! tags group Agents and chats. Execution project, scope, workspace and
//! history are never edited here. `chat.continue` starts a new authorized
//! project session and records a source reference; the source keeps its
//! history and usage attribution. Change readback rides the existing event
//! watermark; there is no organization revision.

use super::*;
use eliot_core::api::{
    ArchivedFilter, ChatAttachRequest, ChatContinueRequest, ChatContinueResponse,
    ChatDetachRequest, ChatOrganizationMutationResponse, ChatOrganizationRequest,
    ChatOrganizationResponse, ChatTagRequest, MAX_ORG_MEMBERS_PER_GROUP, MAX_ORG_MEMBERS_TOTAL,
    OrgGroupView, OrgLinkView, OrgMemberView, OrgSourceRef, SwitchAgent,
};
use std::collections::BTreeMap;

pub(super) const CHAT_ATTACH_METHOD: &str = "chat.attach";
pub(super) const CHAT_DETACH_METHOD: &str = "chat.detach";
pub(super) const CHAT_TAG_METHOD: &str = "chat.tag";
pub(super) const CHAT_CONTINUE_METHOD: &str = "chat.continue";

/// Most retained roots read back for one project group: the first roots the
/// team, the rest surface as the ambiguity. Roots are parentless Agents, so
/// one project never holds many.
const MAX_ORG_ROOTS_PER_GROUP: usize = 65;

fn owner_only(principal: &str, what: &str) -> Result<()> {
    if principal == "owner" {
        Ok(())
    } else {
        Err(Error::new("not_allowed", format!("only Owner may {what}")))
    }
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
    source: Option<(i64, i64)>,
}

fn load_org_rows(conn: &Connection) -> Result<BTreeMap<i64, OrgRow>> {
    let mut statement = conn
        .prepare(
            "SELECT agent, project_link, tags, source, source_generation FROM chat_organization",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<i64>>(3)?,
                row.get::<_, Option<i64>>(4)?,
            ))
        })
        .map_err(sql_error)?;
    let mut map = BTreeMap::new();
    for row in rows {
        let (agent, link, tags, source, source_generation) = row.map_err(sql_error)?;
        let source = source.zip(source_generation);
        map.insert(
            agent,
            OrgRow {
                link,
                tags: decode_tags(&tags)?,
                source,
            },
        );
    }
    Ok(map)
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

/// Retained roots of one execution project: parentless, live-scope,
/// non-chat Agents in id order. The caller picks the archived predicate, so
/// assignment need stays display-scoped while ambiguity counts every
/// retained root independently of the archived filter.
fn organization_roots(conn: &Connection, project: &str, archived: &str) -> Result<Vec<i64>> {
    let mut statement = conn
        .prepare(&format!(
            "SELECT a.id FROM agents a WHERE a.parent IS NULL AND a.observe_only = 0
             AND a.chat = 0 AND a.project = ?1 AND {archived}
             ORDER BY a.id LIMIT {}",
            MAX_ORG_ROOTS_PER_GROUP as i64,
        ))
        .map_err(sql_error)?;
    let rows = statement
        .query_map([project], |row| row.get::<_, i64>(0))
        .map_err(sql_error)?;
    rows.collect::<rusqlite::Result<_>>().map_err(sql_error)
}

/// Bounded member rows for one group: execution members plus chats placed
/// here by the overlay for a project, or the unplaced bucket when `project`
/// is absent. The rowid-ordered scan stops after the group's share, so a
/// single-project read never materializes the fleet.
fn organization_members(
    conn: &Connection,
    project: Option<&str>,
    archived: &str,
    take: usize,
    org_rows: &BTreeMap<i64, OrgRow>,
    members: &mut Vec<OrgMemberView>,
) -> Result<()> {
    let take = i64::try_from(take)
        .map_err(|_| Error::new("internal", "organization page bound is invalid"))?;
    let mut statement = match project {
        Some(_) => conn
            .prepare(&format!(
                "SELECT a.id, a.generation, a.state, a.harness, a.parent, a.root, a.project,
                        a.chat, a.archived, a.observe_only, a.workspace
                 FROM agents a LEFT JOIN chat_organization o ON o.agent = a.id
                 WHERE ((a.project = ?1 AND (a.chat = 0 OR o.project_link IS NULL
                        OR NOT EXISTS(SELECT 1 FROM project_registrations r WHERE r.id = o.project_link)))
                       OR (a.chat = 1 AND o.project_link = ?1))
                   AND {archived}
                 ORDER BY a.id LIMIT ?2",
            ))
            .map_err(sql_error)?,
        None => conn
            .prepare(&format!(
                "SELECT a.id, a.generation, a.state, a.harness, a.parent, a.root, a.project,
                        a.chat, a.archived, a.observe_only, a.workspace
                 FROM agents a LEFT JOIN chat_organization o ON o.agent = a.id
                 WHERE NOT ((a.chat = 1 AND o.project_link IS NOT NULL
                            AND EXISTS(SELECT 1 FROM project_registrations r WHERE r.id = o.project_link))
                           OR EXISTS(SELECT 1 FROM project_registrations r WHERE r.id = a.project))
                   AND {archived}
                 ORDER BY a.id LIMIT ?1",
            ))
            .map_err(sql_error)?,
    };
    let rows: Vec<(
        i64,
        i64,
        String,
        String,
        Option<i64>,
        i64,
        String,
        bool,
        bool,
        bool,
        Option<String>,
    )> = match project {
        Some(name) => statement
            .query_map(params![name, take], |row| {
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
            .map_err(sql_error)?
            .collect::<rusqlite::Result<_>>()
            .map_err(sql_error)?,
        None => statement
            .query_map(params![take], |row| {
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
            .map_err(sql_error)?
            .collect::<rusqlite::Result<_>>()
            .map_err(sql_error)?,
    };
    for (
        num,
        generation,
        state,
        harness,
        parent,
        root,
        execution,
        chat,
        archived,
        _observe_only,
        workspace,
    ) in rows
    {
        let org = org_rows.get(&num);
        let link = org.and_then(|entry| entry.link.clone());
        members.push(OrgMemberView {
            agent: format_agent_id(num),
            generation,
            state: parse_enum(&state, "Agent state")?,
            harness,
            parent: parent.map(format_agent_id),
            root: format_agent_id(root),
            project: execution,
            chat,
            archived,
            project_link: link.clone(),
            tags: org.map(|entry| entry.tags.clone()).unwrap_or_default(),
            placed: chat && link.is_some(),
            continued_from: org.and_then(|entry| {
                entry.source.map(|(source, generation)| OrgSourceRef {
                    agent: format_agent_id(source),
                    generation,
                })
            }),
            workspace,
        });
    }
    Ok(())
}

impl Store {
    /// Grouped work projection: one group per project in the retained union
    /// (registrations, assignments, execution labels) plus the unplaced
    /// bucket. Groups follow project assignment, manager,
    /// repository/worktree and ancestry; chats additionally follow their
    /// placement link. Owner only. Totals come from one aggregate pass and
    /// member rows from index-scoped bounded queries, so a single-project
    /// read never materializes the fleet; the overlay supplies placement,
    /// tags and source references on those bounded rows.
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
        let watermark = organization_watermark(conn)?;
        let union = overview::list_overview_projects(conn, request.project.as_deref(), None)?;
        if request.project.is_some() && union.is_empty() {
            return Err(Error::new(
                "not_found",
                format!(
                    "project {} was not found",
                    request.project.as_deref().unwrap_or_default()
                ),
            ));
        }
        let archived = match request.archived {
            ArchivedFilter::Exclude => "a.archived = 0",
            ArchivedFilter::Only => "a.archived = 1",
            ArchivedFilter::Include => "1 = 1",
        };
        // Exact per-group totals without materializing rows: the key
        // expression mirrors the member queries below.
        let mut counts: BTreeMap<Option<String>, usize> = BTreeMap::new();
        {
            let mut totals = conn
                .prepare(&format!(
                    "SELECT CASE WHEN (a.chat = 1 AND o.project_link IS NOT NULL
                                 AND EXISTS(SELECT 1 FROM project_registrations r WHERE r.id = o.project_link))
                            THEN o.project_link
                            WHEN EXISTS(SELECT 1 FROM project_registrations r WHERE r.id = a.project)
                            THEN a.project ELSE NULL END,
                        COUNT(*)
                     FROM agents a LEFT JOIN chat_organization o ON o.agent = a.id
                     WHERE {archived} GROUP BY 1",
                ))
                .map_err(sql_error)?;
            let rows = totals
                .query_map([], |row| {
                    Ok((row.get::<_, Option<String>>(0)?, row.get::<_, i64>(1)?))
                })
                .map_err(sql_error)?;
            for row in rows {
                let (key, total) = row.map_err(sql_error)?;
                counts.insert(key, total as usize);
            }
        }
        let org_rows = load_org_rows(conn)?;
        let mut response = ChatOrganizationResponse {
            event_watermark: watermark,
            as_of: now,
            groups: Vec::new(),
            member_total: counts.values().sum(),
            truncated: false,
        };
        // Fill in response order (projects first, unplaced last) until the
        // projection cap; totals stay exact and truncation stays explicit.
        let mut budget = MAX_ORG_MEMBERS_TOTAL;
        let mut kept = 0_usize;
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
                        archived,
                    )?);
                }
                continue;
            }
            let take = full.min(MAX_ORG_MEMBERS_PER_GROUP).min(budget);
            budget -= take;
            kept += take;
            let mut members = Vec::with_capacity(take);
            organization_members(
                conn,
                key.as_deref(),
                archived,
                take,
                &org_rows,
                &mut members,
            )?;
            response.groups.push(self.organization_group(
                conn,
                key,
                members,
                full,
                full > take,
                archived,
            )?);
        }
        response.truncated = response.member_total > kept;
        Ok(response)
    }

    fn organization_group(
        &self,
        conn: &Connection,
        project: Option<String>,
        members: Vec<OrgMemberView>,
        member_total: usize,
        members_truncated: bool,
        archived: &str,
    ) -> Result<OrgGroupView> {
        let (registration, assignment, manager, needs_assignment, conflicting_roots) =
            match project.as_deref() {
                Some(project) => {
                    let registration = projects::project_registration_tx(conn, project)?;
                    if registration.is_none() {
                        // Execution labels and assignments without a retained
                        // registration group by label but carry no team detail.
                        (None, None, None, false, Vec::new())
                    } else {
                        let assignment = project_assignment::read_assignment(conn, project)?;
                        let manager = overview::overview_manager_view(conn, Some(&assignment))?;
                        let needs_assignment = assignment.manager.is_none()
                            && !organization_roots(conn, project, archived)?.is_empty();
                        // Competing roots surface without reassigning either
                        // root, counted independently of the display filter.
                        let conflicting_roots = organization_roots(conn, project, "1 = 1")?
                            .into_iter()
                            .skip(1)
                            .map(format_agent_id)
                            .collect();
                        (
                            registration,
                            Some(assignment),
                            manager,
                            needs_assignment,
                            conflicting_roots,
                        )
                    }
                }
                None => (None, None, None, false, Vec::new()),
            };
        Ok(OrgGroupView {
            project,
            registration,
            assignment,
            needs_assignment,
            conflicting_roots,
            manager,
            members,
            member_total,
            members_truncated,
        })
    }

    /// Shared attach/detach/tag transaction: validate everything, then
    /// apply link rows atomically. Placement only: execution project,
    /// scope, workspace and history are untouched. Attach and detach place
    /// chats; tags group any Agent or chat. Replay returns the retained
    /// row; the event watermark is the change readback.
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
                    "INSERT INTO chat_organization(agent, project_link, tags, updated_at)
                     VALUES (?1, ?2, ?3, ?4)
                     ON CONFLICT(agent) DO UPDATE SET project_link = excluded.project_link,
                         tags = excluded.tags, updated_at = excluded.updated_at",
                    params![num, link, tags, now],
                ));
                links.push(OrgLinkView {
                    agent,
                    project_link: link,
                    tags: decode_tags(&tags)?,
                });
            }
            let value = ChatOrganizationMutationResponse {
                id: id.to_owned(),
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
    /// The admitted preparation must still belong to the reserved project's
    /// repository; the registration revision binds the dispatch.
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
            if let Some(preparation) = &preparation
                && preparation.repo_common_dir != registration.repo_common_dir
            {
                return Err(Error::new(
                    "not_allowed",
                    "continue preparation left the project repository",
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
    /// usage attribution; only pointers are retained here, in the Agent's
    /// organization metadata. The readback carries the admitted workspace
    /// and the linked brief's actual outcome.
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
            let brief = load_command(&tx, &plan.brief_command)?
                .ok_or_else(|| Error::new("bad_state", "continue brief not admitted"))?;
            let mut events = Vec::new();
            let state: CommandState;
            let mut error = None;
            let mut created = None;
            let mut workspace = plan.workspace.clone();
            if launch.state == CommandState::Applied && brief.state == CommandState::Applied {
                let target = launch.target.clone().ok_or_else(|| {
                    Error::new("source_integrity", "continue launch has no session")
                })?;
                let session = parse_agent_id(&target)?;
                let agent = load_agent_by_num(&tx, session)?.ok_or_else(|| {
                    Error::new("source_integrity", "continue session disappeared")
                })?;
                let source = parse_agent_id(&plan.source.agent)?;
                db!(tx.execute(
                    "INSERT INTO chat_organization(agent, project_link, tags, source, source_generation, updated_at)
                     VALUES (?1, NULL, '[]', ?2, ?3, ?4)
                     ON CONFLICT(agent) DO UPDATE SET source = excluded.source,
                         source_generation = excluded.source_generation, updated_at = excluded.updated_at",
                    params![session, source, plan.source.generation, now],
                ));
                workspace = agent
                    .workspace
                    .clone()
                    .unwrap_or_else(|| plan.workspace.clone());
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
                ) || (launch.state == CommandState::Applied
                    && matches!(
                        brief.state,
                        CommandState::Accepted
                            | CommandState::Queued
                            | CommandState::Dispatched
                            | CommandState::Unknown
                    )) {
                    CommandState::Unknown
                } else {
                    CommandState::Failed
                };
                let code = if launch.state != CommandState::Applied {
                    "launch_not_ready"
                } else if matches!(
                    brief.state,
                    CommandState::Accepted
                        | CommandState::Queued
                        | CommandState::Dispatched
                        | CommandState::Unknown
                ) {
                    "brief_not_ready"
                } else {
                    "brief_failed"
                };
                error = Some(Error::new(code, "continue brief has no applied outcome"));
            }
            if error.as_ref().is_some_and(|error| error.code == "internal") {
                return Err(error.unwrap());
            }
            let mut value = continue_result_on(&tx, id)?;
            value.error = error.map(|error| error.code);
            value.state = state;
            value.created = created;
            value.workspace = workspace;
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

    /// The new session is ready when its launch applied and the linked
    /// source-reference brief applied: the readback exposes both outcomes.
    pub fn continue_launch_ready(&self, id: &str) -> Result<bool> {
        self.ensure_committed_reads()?;
        let value = continue_result_on(&self.connection, id)?;
        let Some(launch) = load_command(&self.connection, &value.launch_command)? else {
            return Ok(false);
        };
        if launch.state != CommandState::Applied {
            return Ok(false);
        }
        let Some(brief) = load_command(&self.connection, &value.brief_command)? else {
            return Ok(false);
        };
        Ok(brief.state == CommandState::Applied)
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

    fn project_group<'a>(
        projection: &'a ChatOrganizationResponse,
        project: &str,
    ) -> &'a OrgGroupView {
        projection
            .groups
            .iter()
            .find(|group| group.project.as_deref() == Some(project))
            .unwrap_or_else(|| panic!("missing group for project {project}"))
    }

    #[test]
    fn attach_detach_round_trip_with_replay() {
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
        assert_eq!(attached.links.len(), 1);
        // Same id and payload replays instead of writing again.
        let replayed = store.attach_chats("owner", &first, now()).unwrap().value;
        assert_eq!(replayed, attached);

        let detach = ChatDetachRequest {
            id: Ulid::new().to_string(),
            agents: vec![chat.clone()],
        };
        let detached = store.detach_chats("owner", &detach, now()).unwrap().value;
        detach.validate_response(&detached).unwrap();

        // Execution facts are untouched by placement.
        let row = load_agent_by_num(&store.connection, parse_agent_id(&chat).unwrap())
            .unwrap()
            .unwrap();
        assert!(row.project.is_empty());

        // The grouped projection reads the placement back with the event
        // watermark as its change readback.
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
    fn competing_roots_surface_without_reassignment() {
        let mut store = Store::open(Path::new(":memory:")).unwrap();
        register(&mut store, "alpha", "/managed/alpha");
        let first = launch(
            &mut store,
            &Ulid::new().to_string(),
            "alpha",
            "/managed/alpha",
        );
        let second = launch(
            &mut store,
            &Ulid::new().to_string(),
            "alpha",
            "/managed/alpha",
        );
        let first_num = parse_agent_id(&first).unwrap();
        store
            .connection
            .execute(
                "INSERT INTO project_assignments(project, manager, manager_generation, epoch, revision, start_command)
                 VALUES ('alpha', ?1, 0, 1, 1, NULL)",
                [first_num],
            )
            .unwrap();
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
        let alpha = project_group(&projection, "alpha");
        // The assignment stands, so nothing reads as needing assignment, but
        // the second independent root is surfaced explicitly.
        assert!(!alpha.needs_assignment);
        assert_eq!(alpha.conflicting_roots, vec![second.clone()]);
        // Neither root moved: both still group under their execution project.
        assert!(alpha.members.iter().any(|member| member.agent == first));
        assert!(alpha.members.iter().any(|member| member.agent == second));

        // The ambiguity ignores the archived display filter.
        store
            .connection
            .execute(
                "UPDATE agents SET archived = 1 WHERE id = ?1",
                [parse_agent_id(&second).unwrap()],
            )
            .unwrap();
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
        let alpha = project_group(&projection, "alpha");
        assert_eq!(alpha.conflicting_roots, vec![second.clone()]);
        assert!(alpha.members.iter().all(|member| member.agent != second));
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

    #[test]
    fn restart_preserves_typed_continue_outcome_for_replay() {
        let mut store = Store::open(Path::new(":memory:")).unwrap();
        register(&mut store, "alpha", "/managed/alpha");
        let chat = launch_chat(&mut store, &Ulid::new().to_string(), "/tmp/wt-k175-chat-e");
        let request = continue_request(&Ulid::new().to_string(), &chat, "alpha");
        let reserved = store
            .reserve_chat_continue("owner", &request, &core::SecretPolicy::default(), now())
            .unwrap()
            .value;
        store.start_chat_continue(&request.id, now()).unwrap();
        store.recover_on_restart(now()).unwrap();
        // The restart marks the parent unknown but keeps the typed launch,
        // brief and source bindings, so same-ID replay still decodes.
        let readback = store.continue_result(&request.id).unwrap();
        assert_eq!(readback.launch_command, reserved.launch_command);
        assert_eq!(readback.brief_command, reserved.brief_command);
        assert_eq!(readback.source.agent, chat);
        let replayed = store
            .replay_chat_continue("owner", &request)
            .unwrap()
            .expect("restarted continue replays");
        assert_eq!(replayed.launch_command, reserved.launch_command);
        assert_eq!(replayed.state, CommandState::Unknown);
    }

    #[test]
    fn source_reference_reads_back_on_the_overlay() {
        let mut store = Store::open(Path::new(":memory:")).unwrap();
        register(&mut store, "alpha", "/managed/alpha");
        let worker = launch(
            &mut store,
            &Ulid::new().to_string(),
            "alpha",
            "/managed/alpha",
        );
        let session = launch(
            &mut store,
            &Ulid::new().to_string(),
            "alpha",
            "/managed/alpha",
        );
        let worker_num = parse_agent_id(&worker).unwrap();
        let session_num = parse_agent_id(&session).unwrap();
        store
            .connection
            .execute(
                "INSERT INTO chat_organization(agent, project_link, tags, source, source_generation, updated_at)
                 VALUES (?1, NULL, '[]', ?2, 1, 0)",
                params![session_num, worker_num],
            )
            .unwrap();
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
        let member = project_group(&projection, "alpha")
            .members
            .iter()
            .find(|member| member.agent == session)
            .expect("continued session");
        assert_eq!(
            member.continued_from,
            Some(OrgSourceRef {
                agent: worker.clone(),
                generation: 1,
            })
        );
    }

    #[test]
    fn migration_links_only_unambiguous_projectless_chats() {
        let mut store = Store::open(Path::new(":memory:")).unwrap();
        register(&mut store, "alpha", "/managed/alpha");
        register(&mut store, "beta", "/managed/beta");
        let alpha_worker = launch(
            &mut store,
            &Ulid::new().to_string(),
            "alpha",
            "/managed/alpha",
        );
        let beta_worker = launch(
            &mut store,
            &Ulid::new().to_string(),
            "beta",
            "/managed/beta",
        );
        // A projectless chat under a single retained project links to it.
        let lone = launch_chat(&mut store, &Ulid::new().to_string(), "/tmp/wt-k175-lone");
        // Competing ancestry (beta parent under an alpha grandparent) links nowhere.
        let tangled = launch_chat(&mut store, &Ulid::new().to_string(), "/tmp/wt-k175-tangled");
        // A projectless chat whose retained worktree belongs to alpha links to it.
        let worktree_chat = launch_chat(
            &mut store,
            &Ulid::new().to_string(),
            "/tmp/wt-k175-branched",
        );
        store
            .connection
            .execute(
                "UPDATE agents SET parent = ?1 WHERE id = ?2",
                params![
                    parse_agent_id(&alpha_worker).unwrap(),
                    parse_agent_id(&lone).unwrap()
                ],
            )
            .unwrap();
        store
            .connection
            .execute(
                "UPDATE agents SET parent = ?1 WHERE id = ?2",
                params![
                    parse_agent_id(&beta_worker).unwrap(),
                    parse_agent_id(&tangled).unwrap()
                ],
            )
            .unwrap();
        store
            .connection
            .execute(
                "UPDATE agents SET parent = ?1 WHERE id = ?2",
                params![
                    parse_agent_id(&alpha_worker).unwrap(),
                    parse_agent_id(&beta_worker).unwrap()
                ],
            )
            .unwrap();
        let command: String = store
            .connection
            .query_row("SELECT id FROM commands LIMIT 1", [], |row| row.get(0))
            .unwrap();
        store
            .connection
            .execute(
                "INSERT INTO workspaces(agent, target_generation, start_command_id, project, repo_path,
                     repo_common_dir, requested_branch, base_ref, base_oid, worktree_path, frozen_setup,
                     phase, setup_index, outcome, updated_at)
                 VALUES (?1, 1, ?2, '', '/managed/alpha', '/managed/alpha/.git', 'branch',
                     NULL, 'oid', '/tmp/wt-k175-branched', '[]', 'ready', 0, NULL, 0)",
                params![parse_agent_id(&worktree_chat).unwrap(), command],
            )
            .unwrap();
        // Re-run the v63 migration over the populated store.
        store
            .connection
            .execute_batch(
                "DROP TABLE chat_organization; DELETE FROM schema_migrations WHERE version = 63;",
            )
            .unwrap();
        store.migrate().unwrap();
        let link: Option<String> = store
            .connection
            .query_row(
                "SELECT project_link FROM chat_organization WHERE agent = ?1",
                [parse_agent_id(&lone).unwrap()],
                |row| row.get(0),
            )
            .optional()
            .unwrap()
            .flatten();
        assert_eq!(link.as_deref(), Some("alpha"));
        let tangled_link: Option<Option<String>> = store
            .connection
            .query_row(
                "SELECT project_link FROM chat_organization WHERE agent = ?1",
                [parse_agent_id(&tangled).unwrap()],
                |row| row.get(0),
            )
            .optional()
            .unwrap();
        assert!(tangled_link.is_none());
        let branched: Option<String> = store
            .connection
            .query_row(
                "SELECT project_link FROM chat_organization WHERE agent = ?1",
                [parse_agent_id(&worktree_chat).unwrap()],
                |row| row.get(0),
            )
            .optional()
            .unwrap()
            .flatten();
        assert_eq!(branched.as_deref(), Some("alpha"));
    }
}
