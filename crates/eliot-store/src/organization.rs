//! Headless chat and work organization (step 4a): placement overlay, tags
//! and grouped projection.
//!
//! Organization is placement, never authority. Attach/detach move a chat's
//! display placement between a retained project and the no-project bucket;
//! tags group Agents and chats. Execution project, scope, workspace and
//! history are never edited here. Change readback rides the existing event
//! watermark; there is no organization revision.

use super::*;
use eliot_core::api::{
    ArchivedFilter, ChatAttachRequest, ChatDetachRequest, ChatOrganizationMutationResponse,
    ChatOrganizationRequest, ChatOrganizationResponse, ChatTagRequest, MAX_ORG_MEMBERS_PER_GROUP,
    MAX_ORG_MEMBERS_TOTAL, OrgGroupView, OrgLinkView, OrgMemberView, ProjectManagerIdentity,
};
use std::collections::BTreeMap;

pub(super) const CHAT_ATTACH_METHOD: &str = "chat.attach";
pub(super) const CHAT_DETACH_METHOD: &str = "chat.detach";
pub(super) const CHAT_TAG_METHOD: &str = "chat.tag";

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

/// Decode one stored overlay row: the placement link is read back as-is and
/// the tag set is validated against the API bounds.
fn decode_overlay(link: Option<String>, raw_tags: String) -> Result<(Option<String>, Vec<String>)> {
    Ok((link, decode_tags(&raw_tags)?))
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

/// The organization group of one Agent row (aliases `a` for `agents`, `o`
/// for its `chat_organization` overlay): a chat's placement link when it is
/// a retained registration, any other Agent's execution project when that
/// is one, else NULL for the no-project bucket. The projection's per-group
/// totals and every `agent.list` row read this one expression, so a row
/// never reads in a different group than the projection counts it.
const ORGANIZATION_GROUP: &str = "CASE WHEN (a.chat = 1 AND o.project_link IS NOT NULL
         AND EXISTS(SELECT 1 FROM project_registrations r WHERE r.id = o.project_link))
    THEN o.project_link
    WHEN (a.chat = 0
          AND EXISTS(SELECT 1 FROM project_registrations r WHERE r.id = a.project))
    THEN a.project ELSE NULL END";

/// One Agent's organization group ([`ORGANIZATION_GROUP`]) and tags, for
/// its `agent.list` row.
pub(super) fn agent_organization(
    conn: &Connection,
    agent: i64,
) -> Result<(Option<String>, Vec<String>)> {
    let (group, raw_tags): (Option<String>, Option<String>) = conn
        .query_row(
            &format!(
                "SELECT {ORGANIZATION_GROUP}, o.tags
                 FROM agents a LEFT JOIN chat_organization o ON o.agent = a.id
                 WHERE a.id = ?1"
            ),
            [agent],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .map_err(sql_error)?;
    let tags = match raw_tags {
        Some(raw) => decode_tags(&raw)?,
        None => Vec::new(),
    };
    Ok((group, tags))
}

/// Project-scoped member predicate shared by the count and the page: chats
/// group by their overlay link alone (a retained execution label never
/// places a chat), every other Agent by its execution project.
fn organization_project_members() -> &'static str {
    "((a.chat = 0 AND a.project = ?1) OR (a.chat = 1 AND o.project_link = ?1))"
}

/// Unplaced-bucket predicate shared by the count and the page: chats
/// without a link plus non-chat Agents whose execution project is not a
/// retained registration.
fn organization_unplaced_members() -> &'static str {
    "((a.chat = 1 AND o.project_link IS NULL)
      OR (a.chat = 0 AND NOT EXISTS(SELECT 1 FROM project_registrations r WHERE r.id = a.project)))"
}

/// Exact member total for one group through the same project-scoped
/// predicates the page below uses, so a single-project read never
/// aggregates the fleet.
fn organization_count(conn: &Connection, project: Option<&str>, archived: &str) -> Result<usize> {
    let sql = |predicate: &str| {
        format!(
            "SELECT COUNT(*) FROM agents a LEFT JOIN chat_organization o ON o.agent = a.id
             WHERE {archived} AND {predicate}",
        )
    };
    let total: i64 = match project {
        Some(name) => conn
            .query_row(&sql(organization_project_members()), [name], |row| {
                row.get(0)
            })
            .map_err(sql_error)?,
        None => conn
            .query_row(&sql(organization_unplaced_members()), [], |row| row.get(0))
            .map_err(sql_error)?,
    };
    Ok(total as usize)
}

/// One group page in response order with its exact per-group totals and
/// the group-truncation state.
struct OrganizationPage<'a> {
    order: Vec<Option<String>>,
    counts: &'a BTreeMap<Option<String>, usize>,
    archived: &'a str,
    watermark: i64,
    now: i64,
    groups_truncated: bool,
    next_after: Option<String>,
}

/// Bounded member rows for one group: execution members for a project, or
/// chats placed here by the overlay alone; the unplaced bucket when
/// `project` is absent. The id-ordered scan stops after the group's share
/// and decodes the overlay link and tags inline, so a single-project read
/// never materializes the fleet or the whole overlay table.
type OrgMemberRow = (
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
    Option<String>,
    Option<String>,
);

fn organization_members(
    conn: &Connection,
    project: Option<&str>,
    archived: &str,
    take: usize,
    members: &mut Vec<OrgMemberView>,
) -> Result<()> {
    let take = i64::try_from(take)
        .map_err(|_| Error::new("internal", "organization page bound is invalid"))?;
    let project_predicate = organization_project_members();
    let unplaced_predicate = organization_unplaced_members();
    let mut statement = match project {
        Some(_) => conn
            .prepare(&format!(
                "SELECT a.id, a.generation, a.state, a.harness, a.parent, a.root, a.project,
                        a.chat, a.archived, a.observe_only, a.workspace,
                        o.project_link, o.tags
                 FROM agents a LEFT JOIN chat_organization o ON o.agent = a.id
                 WHERE {project_predicate}
                   AND {archived}
                 ORDER BY a.id LIMIT ?2",
            ))
            .map_err(sql_error)?,
        None => conn
            .prepare(&format!(
                "SELECT a.id, a.generation, a.state, a.harness, a.parent, a.root, a.project,
                        a.chat, a.archived, a.observe_only, a.workspace,
                        o.project_link, o.tags
                 FROM agents a LEFT JOIN chat_organization o ON o.agent = a.id
                 WHERE {unplaced_predicate}
                   AND {archived}
                 ORDER BY a.id LIMIT ?1",
            ))
            .map_err(sql_error)?,
    };
    let rows: Vec<OrgMemberRow> = match project {
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
                    row.get::<_, Option<String>>(11)?,
                    row.get::<_, Option<String>>(12)?,
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
                    row.get::<_, Option<String>>(11)?,
                    row.get::<_, Option<String>>(12)?,
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
        link,
        raw_tags,
    ) in rows
    {
        let (project_link, tags) =
            decode_overlay(link, raw_tags.unwrap_or_else(|| "[]".to_owned()))?;
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
            placed: chat && project_link.is_some(),
            project_link,
            tags,
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
    /// placement link. Owner only. Counts and member rows run through the
    /// same project-scoped predicates, so a single-project read never
    /// aggregates the fleet; the overlay supplies placement and tags on
    /// those bounded rows. The all-project read pages its group list with
    /// an `after` cursor and explicit truncation.
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
        // Existence ignores the cursor: a single-project read names its
        // group even when the cursor already passed it.
        if request.project.is_some()
            && overview::list_overview_projects(conn, request.project.as_deref(), None)?.is_empty()
        {
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
        if let Some(project) = request.project.as_deref() {
            return self.single_project_organization(
                conn,
                project,
                request.after.as_deref(),
                archived,
                watermark,
                now,
            );
        }
        self.all_projects_organization(conn, request.after.as_deref(), archived, watermark, now)
    }

    /// One project group (or an empty page past the cursor) with its exact
    /// total from the same scoped predicate the member page uses.
    fn single_project_organization(
        &self,
        conn: &Connection,
        project: &str,
        after: Option<&str>,
        archived: &str,
        watermark: i64,
        now: i64,
    ) -> Result<ChatOrganizationResponse> {
        let mut counts = BTreeMap::new();
        let mut order = Vec::new();
        // The cursor is positional: a project sorts at or before it when
        // the page is already past.
        if after.is_none_or(|after| after < project) {
            counts.insert(
                Some(project.to_owned()),
                organization_count(conn, Some(project), archived)?,
            );
            order.push(Some(project.to_owned()));
        }
        self.organization_page(
            conn,
            OrganizationPage {
                order,
                counts: &counts,
                archived,
                watermark,
                now,
                groups_truncated: false,
                next_after: None,
            },
        )
    }

    /// Every group in page order (projects first, unplaced last) with exact
    /// per-group totals from one aggregate pass. `list_overview_projects`
    /// returns one row past the page bound, so a longer group list reads
    /// back truncated with the cursor for the next page.
    fn all_projects_organization(
        &self,
        conn: &Connection,
        after: Option<&str>,
        archived: &str,
        watermark: i64,
        now: i64,
    ) -> Result<ChatOrganizationResponse> {
        let mut keys = overview::list_overview_projects(conn, None, after)?;
        let groups_truncated = keys.len() > MAX_PAGE_ENTRIES;
        if groups_truncated {
            keys.truncate(MAX_PAGE_ENTRIES);
        }
        // Exact per-group totals without materializing rows: the key
        // expression mirrors the member queries below. Chats group by
        // their overlay link alone; a retained execution label never
        // places a chat.
        let mut counts: BTreeMap<Option<String>, usize> = BTreeMap::new();
        {
            let mut totals = conn
                .prepare(&format!(
                    "SELECT {ORGANIZATION_GROUP}, COUNT(*)
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
        let next_after = groups_truncated.then(|| {
            keys.last()
                .expect("truncated group page carries its last project")
                .clone()
        });
        let mut order: Vec<Option<String>> = keys.into_iter().map(Some).collect();
        if !groups_truncated {
            order.push(None);
        }
        self.organization_page(
            conn,
            OrganizationPage {
                order,
                counts: &counts,
                archived,
                watermark,
                now,
                groups_truncated,
                next_after,
            },
        )
    }

    /// Fill one page in response order until the member cap; totals stay
    /// exact and both member and group truncation stay explicit.
    fn organization_page(
        &self,
        conn: &Connection,
        page: OrganizationPage<'_>,
    ) -> Result<ChatOrganizationResponse> {
        let mut budget = MAX_ORG_MEMBERS_TOTAL;
        let mut kept = 0_usize;
        let member_total: usize = page
            .order
            .iter()
            .map(|key| page.counts.get(key).copied().unwrap_or(0))
            .sum();
        let mut response = ChatOrganizationResponse {
            event_watermark: page.watermark,
            as_of: page.now,
            groups: Vec::new(),
            member_total,
            truncated: false,
            groups_truncated: page.groups_truncated,
            next_after: page.next_after,
        };
        for key in page.order {
            let full = page.counts.get(&key).copied().unwrap_or(0);
            if full == 0 {
                // Empty retained projects still read back their team detail.
                if key.is_some() {
                    response.groups.push(self.organization_group(
                        conn,
                        key,
                        Vec::new(),
                        full,
                        false,
                        page.archived,
                    )?);
                }
                continue;
            }
            let take = full.min(MAX_ORG_MEMBERS_PER_GROUP).min(budget);
            budget -= take;
            kept += take;
            let mut members = Vec::with_capacity(take);
            organization_members(conn, key.as_deref(), page.archived, take, &mut members)?;
            response.groups.push(self.organization_group(
                conn,
                key,
                members,
                full,
                full > take,
                page.archived,
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
        let transfer = match project.as_deref() {
            Some(project) if projects::project_registration_tx(conn, project)?.is_some() => {
                project_transfer::open_intent(conn, project)?
            }
            _ => None,
        };
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
                        // The assigned manager is excluded by id: when the
                        // manager is an older or newer root, the other
                        // independent roots still surface.
                        let manager_num =
                            assignment
                                .manager
                                .as_ref()
                                .and_then(|manager| match manager {
                                    ProjectManagerIdentity::Local { agent, .. } => {
                                        parse_agent_id(agent).ok()
                                    }
                                    ProjectManagerIdentity::Remote { .. } => None,
                                });
                        let roots = organization_roots(conn, project, "1 = 1")?;
                        let conflicting_roots = match manager_num {
                            Some(manager) => roots
                                .into_iter()
                                .filter(|root| *root != manager)
                                .map(format_agent_id)
                                .collect(),
                            None => roots.into_iter().skip(1).map(format_agent_id).collect(),
                        };
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
            transfer,
            members,
            member_total,
            members_truncated,
        })
    }

    /// Shared attach/detach/tag transaction: validate everything, then
    /// apply link rows atomically. Placement only: execution project,
    /// scope, workspace and history are untouched. Attach and detach place
    /// chats; tags group any Agent or chat. Replay returns the retained
    /// row with no new event; a fresh write appends one
    /// `organization.changed` event in the same transaction, so the
    /// projection's event watermark advances and the router publishes it
    /// after commit.
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
                    "INSERT INTO chat_organization(agent, project_link, tags)
                     VALUES (?1, ?2, ?3)
                     ON CONFLICT(agent) DO UPDATE SET project_link = excluded.project_link,
                         tags = excluded.tags",
                    params![num, link, tags],
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
            // The projection advertises the event watermark as its change
            // readback, so the overlay write appends an event in the same
            // transaction; the router publishes it after commit.
            let event = insert_daemon_event(
                &tx,
                now,
                "organization.changed",
                "organization",
                None,
                None,
                serde_json::json!({"id": id, "method": method}),
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
                    after: None,
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
                    after: None,
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
                    after: None,
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
        // The `agent.list` row reads in the same group as the projection,
        // with its tags; an unregistered execution label reads unplaced.
        let legacy = launch(
            &mut store,
            &Ulid::new().to_string(),
            "/legacy/wt-k1",
            "/legacy/wt-k1",
        );
        let tag = ChatTagRequest {
            id: Ulid::new().to_string(),
            agents: vec![chat.clone()],
            tags: vec!["inbox".to_owned()],
        };
        store.tag_agents("owner", &tag, now()).unwrap();
        let rows = store
            .list_agent_summaries(
                &crate::AgentListFilter::default(),
                None,
                100,
                now(),
                &eliot_core::SessionGrowthThresholds::default(),
            )
            .unwrap();
        let row = |id: &str| {
            rows.summaries
                .iter()
                .find(|row| row.id == id)
                .cloned()
                .expect("listed row")
        };
        let chat_row = row(&chat);
        assert_eq!(chat_row.organization.as_deref(), Some("alpha"));
        assert_eq!(chat_row.tags, vec!["inbox"]);
        chat_row.validate().unwrap();
        assert_eq!(row(&legacy).organization, None);
    }

    #[test]
    fn legacy_project_bearing_chat_reads_back_and_detaches_to_unplaced() {
        let mut store = Store::open(Path::new(":memory:")).unwrap();
        register(&mut store, "alpha", "/managed/alpha");
        let chat = launch_chat(&mut store, &Ulid::new().to_string(), "/tmp/wt-k175-legacy");
        // A legacy chat carries a retained execution project without a link.
        store
            .connection
            .execute(
                "UPDATE agents SET project = 'alpha' WHERE id = ?1",
                [parse_agent_id(&chat).unwrap()],
            )
            .unwrap();
        let request = ChatOrganizationRequest {
            project: None,
            after: None,
            archived: ArchivedFilter::Exclude,
        };
        let projection = store.chat_organization("owner", &request, now()).unwrap();
        request.validate_response(&projection).unwrap();
        let unplaced = projection
            .groups
            .iter()
            .find(|group| group.project.is_none())
            .expect("unplaced bucket");
        let member = unplaced
            .members
            .iter()
            .find(|member| member.agent == chat)
            .expect("legacy chat reads unplaced");
        assert_eq!(member.project, "alpha");
        assert!(!member.placed);
        // Attach places it; detach returns it to the unplaced bucket,
        // never under its retained execution project.
        let attach = attach_request(&Ulid::new().to_string(), &[&chat], "alpha");
        store.attach_chats("owner", &attach, now()).unwrap();
        let projection = store.chat_organization("owner", &request, now()).unwrap();
        request.validate_response(&projection).unwrap();
        let alpha = project_group(&projection, "alpha");
        let placed = alpha
            .members
            .iter()
            .find(|member| member.agent == chat)
            .expect("placed chat");
        assert!(placed.placed);
        let detach = ChatDetachRequest {
            id: Ulid::new().to_string(),
            agents: vec![chat.clone()],
        };
        store.detach_chats("owner", &detach, now()).unwrap();
        let projection = store.chat_organization("owner", &request, now()).unwrap();
        request.validate_response(&projection).unwrap();
        let alpha = project_group(&projection, "alpha");
        assert!(alpha.members.iter().all(|member| member.agent != chat));
        let unplaced = projection
            .groups
            .iter()
            .find(|group| group.project.is_none())
            .expect("unplaced bucket");
        assert!(unplaced.members.iter().any(|member| member.agent == chat));
    }

    #[test]
    fn overlay_mutation_advances_the_event_watermark() {
        let mut store = Store::open(Path::new(":memory:")).unwrap();
        register(&mut store, "alpha", "/managed/alpha");
        let chat = launch_chat(
            &mut store,
            &Ulid::new().to_string(),
            "/tmp/wt-k175-watermark",
        );
        let read = |store: &Store| {
            store
                .chat_organization(
                    "owner",
                    &ChatOrganizationRequest {
                        project: None,
                        after: None,
                        archived: ArchivedFilter::Exclude,
                    },
                    now(),
                )
                .unwrap()
                .event_watermark
        };
        let before = read(&store);
        let first = attach_request(&Ulid::new().to_string(), &[&chat], "alpha");
        let change = store.attach_chats("owner", &first, now()).unwrap();
        assert_eq!(change.events.len(), 1);
        assert_eq!(change.events[0].kind, "organization.changed");
        let after = read(&store);
        assert!(after > before);
        assert_eq!(after, change.events[0].seq);
        // Replay retains the row with no new event.
        let replayed = store.attach_chats("owner", &first, now()).unwrap();
        assert!(replayed.events.is_empty());
        assert_eq!(read(&store), after);
    }

    #[test]
    fn group_list_pages_with_cursor_and_explicit_truncation() {
        let mut store = Store::open(Path::new(":memory:")).unwrap();
        for index in 0..MAX_PAGE_ENTRIES + 2 {
            let name = format!("page-{index:04}");
            register(&mut store, &name, &format!("/managed/{name}"));
        }
        let chat = launch_chat(&mut store, &Ulid::new().to_string(), "/tmp/wt-k175-paged");
        let request = ChatOrganizationRequest {
            project: None,
            after: None,
            archived: ArchivedFilter::Exclude,
        };
        let first = store.chat_organization("owner", &request, now()).unwrap();
        request.validate_response(&first).unwrap();
        assert_eq!(first.groups.len(), MAX_PAGE_ENTRIES);
        assert!(first.groups_truncated);
        assert_eq!(first.next_after.as_deref(), Some("page-0999"));
        let request = ChatOrganizationRequest {
            after: first.next_after.clone(),
            ..request
        };
        let second = store.chat_organization("owner", &request, now()).unwrap();
        request.validate_response(&second).unwrap();
        assert!(!second.groups_truncated);
        assert_eq!(second.next_after, None);
        let names: Vec<Option<&str>> = second
            .groups
            .iter()
            .map(|group| group.project.as_deref())
            .collect();
        assert_eq!(names, vec![Some("page-1000"), Some("page-1001"), None]);
        let unplaced = second
            .groups
            .last()
            .expect("unplaced bucket closes the pages");
        assert!(unplaced.members.iter().any(|member| member.agent == chat));
        // A cursor past the last project still closes with the unplaced
        // bucket; the page carries no cursor when nothing follows it.
        let request = ChatOrganizationRequest {
            after: Some("page-1001".to_owned()),
            ..request
        };
        let past = store.chat_organization("owner", &request, now()).unwrap();
        request.validate_response(&past).unwrap();
        assert_eq!(past.groups.len(), 1);
        assert!(past.groups[0].project.is_none());
        assert!(
            past.groups[0]
                .members
                .iter()
                .any(|member| member.agent == chat)
        );
        assert!(!past.groups_truncated);
    }

    #[test]
    fn single_project_read_counts_through_its_own_predicates() {
        let mut store = Store::open(Path::new(":memory:")).unwrap();
        register(&mut store, "alpha", "/managed/alpha");
        register(&mut store, "beta", "/managed/beta");
        let alpha_worker = launch(
            &mut store,
            &Ulid::new().to_string(),
            "alpha",
            "/managed/alpha",
        );
        let _beta_worker = launch(
            &mut store,
            &Ulid::new().to_string(),
            "beta",
            "/managed/beta",
        );
        let chat = launch_chat(&mut store, &Ulid::new().to_string(), "/tmp/wt-k175-scoped");
        let attach = attach_request(&Ulid::new().to_string(), &[&chat], "alpha");
        store.attach_chats("owner", &attach, now()).unwrap();
        for project in ["alpha", "beta"] {
            let request = ChatOrganizationRequest {
                project: Some(project.to_owned()),
                after: None,
                archived: ArchivedFilter::Exclude,
            };
            let projection = store.chat_organization("owner", &request, now()).unwrap();
            request.validate_response(&projection).unwrap();
            assert_eq!(projection.groups.len(), 1);
            assert_eq!(projection.member_total, projection.groups[0].members.len());
            assert!(!projection.groups[0].members_truncated);
        }
        let request = ChatOrganizationRequest {
            project: Some("alpha".to_owned()),
            after: None,
            archived: ArchivedFilter::Exclude,
        };
        let projection = store.chat_organization("owner", &request, now()).unwrap();
        let members: Vec<&str> = projection.groups[0]
            .members
            .iter()
            .map(|member| member.agent.as_str())
            .collect();
        assert_eq!(members, vec![alpha_worker.as_str(), chat.as_str()]);
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
                    after: None,
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
                    after: None,
                    archived: ArchivedFilter::Exclude,
                },
                now(),
            )
            .unwrap();
        let alpha = project_group(&projection, "alpha");
        assert_eq!(alpha.conflicting_roots, vec![second.clone()]);
        assert!(alpha.members.iter().all(|member| member.agent != second));

        // When the assigned manager is the newer root, the older
        // independent root still surfaces as the conflict.
        store
            .connection
            .execute(
                "UPDATE project_assignments SET manager = ?1 WHERE project = 'alpha'",
                [parse_agent_id(&second).unwrap()],
            )
            .unwrap();
        let projection = store
            .chat_organization(
                "owner",
                &ChatOrganizationRequest {
                    project: Some("alpha".to_owned()),
                    after: None,
                    archived: ArchivedFilter::Exclude,
                },
                now(),
            )
            .unwrap();
        let alpha = project_group(&projection, "alpha");
        assert_eq!(alpha.conflicting_roots, vec![first.clone()]);
    }

    #[test]
    fn migration_links_only_unambiguous_chats() {
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
        // A project-bearing chat whose retained worktree points at another
        // project is ambiguous and links nowhere.
        let conflicted = launch_chat(
            &mut store,
            &Ulid::new().to_string(),
            "/tmp/wt-k175-conflict",
        );
        store
            .connection
            .execute(
                "UPDATE agents SET project = 'alpha' WHERE id = ?1",
                [parse_agent_id(&conflicted).unwrap()],
            )
            .unwrap();
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
        store
            .connection
            .execute(
                "INSERT INTO workspaces(agent, target_generation, start_command_id, project, repo_path,
                     repo_common_dir, requested_branch, base_ref, base_oid, worktree_path, frozen_setup,
                     phase, setup_index, outcome, updated_at)
                 VALUES (?1, 1, ?2, '', '/managed/beta', '/managed/beta/.git', 'branch',
                     NULL, 'oid', '/tmp/wt-k175-conflict', '[]', 'ready', 0, NULL, 0)",
                params![parse_agent_id(&conflicted).unwrap(), command],
            )
            .unwrap();
        // Re-run the v64 migration over the populated store.
        store
            .connection
            .execute_batch(
                "DROP TABLE chat_organization; DELETE FROM schema_migrations WHERE version = 64;",
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
        // The project-bearing chat with conflicting retained evidence
        // links nowhere: one distinct candidate is required.
        let conflicted_link: Option<Option<String>> = store
            .connection
            .query_row(
                "SELECT project_link FROM chat_organization WHERE agent = ?1",
                [parse_agent_id(&conflicted).unwrap()],
                |row| row.get(0),
            )
            .optional()
            .unwrap();
        assert!(conflicted_link.is_none());
    }
}
