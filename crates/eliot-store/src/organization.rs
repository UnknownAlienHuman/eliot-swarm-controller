//! Headless chat and work organization (step 4a): placement overlay, tags
//! and grouped projection.
//!
//! Organization is placement, never authority. Attach/detach move a chat's
//! display placement between a retained project and the no-project bucket;
//! tags group Agents and chats. Execution project, scope, workspace and
//! history are never edited here. Group readback rides the projection's
//! event watermark; each overlay row carries its own link revision, and a
//! menu write whose revision moved is refused.

use super::*;
use eliot_core::api::{
    ArchivedFilter, ChatAttachRequest, ChatDetachRequest, ChatOrganizationMutationResponse,
    ChatOrganizationRequest, ChatOrganizationResponse, ChatTagRequest, MAX_ORG_MEMBERS_PER_GROUP,
    MAX_ORG_MEMBERS_TOTAL, ORGANIZATION_CHANGED_KIND, OrgGroupView, OrgLinkView, OrgMemberView,
    OrgRowChange, OrganizationChangedEvent, ProjectManagerIdentity, ScopeLimits,
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

/// The default archived exclusion, the rule `agent.list` applies: unarchived
/// rows plus custody-held archived rows in an ended state
/// ([`agent_lifecycle::CUSTODY_VISIBLE_STATES`]). The projection's counts
/// and member pages read through it, so a listed row is always counted.
/// Custody ids are integers read from the database, so inlining them is
/// exact.
fn default_visible(conn: &Connection) -> Result<String> {
    let held = agent_lifecycle::default_list_custody_agents(conn)?;
    if held.is_empty() {
        return Ok("a.archived = 0".to_owned());
    }
    let ids = held
        .iter()
        .map(|id| id.to_string())
        .collect::<Vec<_>>()
        .join(",");
    Ok(format!(
        "(a.archived = 0 OR (a.archived = 1 AND a.state IN ({}) AND a.id IN ({ids})))",
        agent_lifecycle::CUSTODY_VISIBLE_STATES,
    ))
}

pub(super) fn decode_tags(raw: &str) -> Result<Vec<String>> {
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
/// the tag set is validated against the API bounds. The continuation
/// pointer is plain display text (possibly naming a deleted source), so it
/// only needs the Agent id shape, never liveness.
fn decode_overlay(
    link: Option<String>,
    raw_tags: String,
    continued_from: Option<String>,
) -> Result<(Option<String>, Vec<String>, Option<String>)> {
    if let Some(source) = &continued_from {
        crate::parse_agent_id(source)?;
    }
    Ok((link, decode_tags(&raw_tags)?, continued_from))
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

    fn expected_revision(&self) -> &Option<Vec<i64>> {
        match self {
            LinkRequest::Attach(request) => &request.expected_revision,
            LinkRequest::Detach(request) => &request.expected_revision,
            LinkRequest::Tag(request) => &request.expected_revision,
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

/// Retained controlled roots grouped by the same evidence as their rows.
fn organization_roots(conn: &Connection, project: &str, archived: &str) -> Result<Vec<i64>> {
    let mut statement = conn
        .prepare(&format!(
            "SELECT a.id FROM agents a WHERE a.parent IS NULL AND a.observe_only = 0
             AND a.chat = 0 AND {} = ?1 AND {archived}
             ORDER BY a.id LIMIT ?2",
            retained_project("a")
        ))
        .map_err(sql_error)?;
    let rows = statement
        .query_map(params![project, MAX_ORG_ROOTS_PER_GROUP as i64], |row| {
            row.get(0)
        })
        .map_err(sql_error)?;
    rows.collect::<rusqlite::Result<_>>().map_err(sql_error)
}

/// Potential independent writers in a registered project. Ambiguous legacy
/// roots are included for every matching project so that absence of a
/// presentation link cannot make their custody disappear.
pub(super) fn retained_root_writers(
    conn: &Connection,
    project: &str,
    except: Option<i64>,
    limit: i64,
) -> Result<Vec<i64>> {
    let matches = legacy_project_matches("a");
    let mut statement = conn
        .prepare_cached(&format!(
            "SELECT a.id FROM agents a WHERE a.parent IS NULL AND a.observe_only=0 AND a.chat=0
               AND a.id IS NOT ?2
               AND EXISTS (
                   SELECT 1 FROM agents member WHERE member.observe_only=0
                   AND (member.id=a.id OR member.root=a.id)
                   AND (member.state IN ('created','starting','idle','busy','waiting','unknown')
                        OR member.capacity_generation IS NOT NULL
                        OR EXISTS (SELECT 1 FROM processes p WHERE p.state <> 'exited'
                            AND p.owner IN ('agent:a' || member.id || '#' || member.generation,
                                            'workspace:a' || member.id || '#' || member.generation))))
               AND (a.project=?1 OR (NOT EXISTS (
                    SELECT 1 FROM project_registrations known WHERE known.id=a.project)
                    AND ?1 IN ({matches})))
             ORDER BY a.id LIMIT ?3",
        ))
        .map_err(sql_error)?;
    let rows = statement
        .query_map(params![project, except, limit], |row| row.get(0))
        .map_err(sql_error)?;
    rows.collect::<rusqlite::Result<_>>().map_err(sql_error)
}

fn checkout_key_sql(column: &str) -> String {
    if cfg!(windows) {
        format!(r"lower(rtrim(replace(replace({column}, '\\?\', ''), '\', '/'), '/'))")
    } else {
        format!("rtrim({column}, '/')")
    }
}

/// Functional indexes persist the normalized registration keys. Exact
/// worktree lookups use the existing unique-path index and owner primary key.
pub(super) fn registration_key_indexes() -> String {
    format!(
        "CREATE INDEX IF NOT EXISTS organization_registration_checkout ON project_registrations({});
         CREATE INDEX IF NOT EXISTS organization_registration_common ON project_registrations({});",
        checkout_key_sql("checkout"),
        checkout_key_sql("repo_common_dir"),
    )
}

fn legacy_project_matches(agent: &str) -> String {
    let project = checkout_key_sql(&format!("{agent}.project"));
    let workspace = checkout_key_sql(&format!("{agent}.workspace"));
    let checkout = checkout_key_sql("r.checkout");
    let worktree = checkout_key_sql("w.worktree_path");
    let common = checkout_key_sql("w.repo_common_dir");
    let registered_common = checkout_key_sql("r.repo_common_dir");
    format!(
        "SELECT r.id FROM project_registrations r INDEXED BY organization_registration_checkout
         WHERE {checkout} IN ({project}, {workspace})
         UNION
         SELECT r.id FROM workspaces w
         CROSS JOIN project_registrations r INDEXED BY organization_registration_common
            ON {registered_common}={common}
         WHERE w.agent={agent}.id AND {worktree}={workspace}
         UNION
         SELECT r.id FROM workspaces w
         CROSS JOIN project_registrations r INDEXED BY organization_registration_common
            ON {registered_common}={common}
         WHERE w.worktree_path={agent}.workspace"
    )
}

/// NULL means no local evidence and permits parent fallback. An empty
/// result records ambiguity and blocks fallback until the final NULLIF.
fn local_project(agent: &str) -> String {
    let matches = legacy_project_matches(agent);
    format!(
        "CASE WHEN EXISTS (SELECT 1 FROM project_registrations r WHERE r.id={agent}.project)
         THEN {agent}.project ELSE (
             SELECT CASE COUNT(*) WHEN 0 THEN NULL WHEN 1 THEN MIN(id) ELSE '' END
             FROM ({matches}))
         END"
    )
}

fn retained_project(agent: &str) -> String {
    let own = local_project(agent);
    let parent = local_project("parent");
    let root = local_project("root");
    format!(
        "NULLIF(COALESCE({own},
            (SELECT COALESCE({parent},{root}) FROM agents parent
             LEFT JOIN agents root ON root.id={agent}.root AND root.id<>parent.id
                  AND root.parent IS NULL AND root.chat=0
             WHERE parent.id={agent}.parent AND parent.id<>{agent}.id
                   AND parent.root={agent}.root AND parent.chat=0)), '')"
    )
}

/// One grouping key for pages, totals, and Agent list readback. Execution
/// labels and the scope that authorizes effects stay on the retained Agent.
pub(super) fn organization_group() -> String {
    format!(
        "CASE WHEN a.chat=1 THEN (SELECT r.id FROM project_registrations r WHERE r.id=o.project_link)
              ELSE {} END",
        retained_project("a")
    )
}

/// One Agent's organization group ([`organization_group`]), tags and overlay
/// revision, for its `agent.list` row and its `organization.changed` event
/// row. The revision is `None` when the Agent has no overlay row; a menu
/// write carries that back as revision 0.
pub(super) fn agent_organization(
    conn: &Connection,
    agent: i64,
) -> Result<(Option<String>, Vec<String>, Option<i64>)> {
    let (group, raw_tags, revision): (Option<String>, Option<String>, Option<i64>) = conn
        .query_row(
            &format!(
                "SELECT {}, o.tags, o.revision
                 FROM agents a LEFT JOIN chat_organization o ON o.agent = a.id
                 WHERE a.id = ?1",
                organization_group()
            ),
            [agent],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .map_err(sql_error)?;
    let tags = match raw_tags {
        Some(raw) => decode_tags(&raw)?,
        None => Vec::new(),
    };
    Ok((group, tags, revision))
}

/// Exact member total for one group through the same project-scoped
/// predicates the page below uses, so a single-project read never
/// aggregates the fleet.
fn organization_count(
    conn: &Connection,
    project: Option<&str>,
    read: &GroupRead<'_>,
) -> Result<usize> {
    let archived = read.archived;
    let sql = |predicate: &str, first: usize| {
        format!(
            "SELECT COUNT(*) FROM agents a LEFT JOIN chat_organization o ON o.agent = a.id
             WHERE {archived} AND {predicate}{}",
            read.query_suffix(first)
        )
    };
    let total: i64 = match project {
        Some(name) => {
            let predicate = format!("{} = ?1", organization_group());
            let mut statement = conn.prepare(&sql(&predicate, 2)).map_err(sql_error)?;
            let mut params: Vec<rusqlite::types::Value> = vec![name.to_owned().into()];
            params.extend(read.query_params());
            statement
                .query_row(rusqlite::params_from_iter(params.iter()), |row| row.get(0))
                .map_err(sql_error)?
        }
        None => {
            let mut statement = conn
                .prepare(&sql(&format!("{} IS NULL", organization_group()), 1))
                .map_err(sql_error)?;
            statement
                .query_row(
                    rusqlite::params_from_iter(read.query_params().iter()),
                    |row| row.get(0),
                )
                .map_err(sql_error)?
        }
    };
    Ok(total as usize)
}

/// What every group of one projection read shares: the archived
/// predicate, the Owner limits for the project slot use, and the snapshot.
struct GroupRead<'a> {
    archived: &'a str,
    limits: &'a ScopeLimits,
    watermark: i64,
    now: i64,
    /// The search (`chat.organization` `query`), or `None`.
    query: Option<&'a str>,
}

impl GroupRead<'_> {
    /// The member-search predicate tail after the scope and archived
    /// predicates, its words bound to the numbered parameters `?first`
    /// onward ([`Self::query_params`] in order); empty without a query.
    /// Explicit numbers keep a later `?N` (the page `LIMIT`) from sharing
    /// a bare placeholder's slot.
    fn query_suffix(&self, first: usize) -> String {
        let words = self.query_params().len();
        if words == 0 {
            return String::new();
        }
        format!(
            " AND {}",
            fleet::agent_search_predicate("a", (first..first + words).map(|n| format!("?{n}")))
        )
    }

    fn query_params(&self) -> Vec<rusqlite::types::Value> {
        self.query.map(fleet::search_words).unwrap_or_default()
    }
}

/// One group page in response order with its exact per-group totals and
/// the group-truncation state.
struct OrganizationPage<'a> {
    order: Vec<Option<String>>,
    counts: &'a BTreeMap<Option<String>, usize>,
    read: &'a GroupRead<'a>,
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
    Option<String>,
);

fn organization_members(
    conn: &Connection,
    project: Option<&str>,
    read: &GroupRead<'_>,
    take: usize,
    members: &mut Vec<OrgMemberView>,
) -> Result<()> {
    let archived = read.archived;
    let take = i64::try_from(take)
        .map_err(|_| Error::new("internal", "organization page bound is invalid"))?;
    let group = organization_group();
    let (condition, limit, first_query_param) = match project {
        Some(_) => ("= ?1", "?2", 3),
        None => ("IS NULL", "?1", 2),
    };
    // Stream membership in Agent id order. The inner limit selects the
    // page's IDs before fetching member fields. Counts stream the same
    // indexed membership lookup without materializing all evidence.
    let mut statement = conn
        .prepare(&format!(
            "SELECT a.id, a.generation, a.state, a.harness, a.parent, a.root, a.project,
                    a.chat, a.archived, a.observe_only, a.workspace,
                    o.project_link, o.tags, o.continued_from
             FROM (
                 SELECT a.id FROM agents a LEFT JOIN chat_organization o ON o.agent=a.id
                 WHERE {group} {condition} AND {archived}{}
                 ORDER BY a.id LIMIT {limit}
             ) page JOIN agents a ON a.id=page.id
             LEFT JOIN chat_organization o ON o.agent=a.id
             ORDER BY a.id",
            read.query_suffix(first_query_param)
        ))
        .map_err(sql_error)?;
    let mut params: Vec<rusqlite::types::Value> = match project {
        Some(name) => vec![name.to_owned().into(), take.into()],
        None => vec![take.into()],
    };
    params.extend(read.query_params());
    let rows: Vec<OrgMemberRow> = statement
        .query_map(rusqlite::params_from_iter(params.iter()), |row| {
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
                row.get::<_, Option<String>>(13)?,
            ))
        })
        .map_err(sql_error)?
        .collect::<rusqlite::Result<_>>()
        .map_err(sql_error)?;
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
        continued_from,
    ) in rows
    {
        let (project_link, tags, continued_from) = decode_overlay(
            link,
            raw_tags.unwrap_or_else(|| "[]".to_owned()),
            continued_from,
        )?;
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
            continued_from,
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
        limits: &ScopeLimits,
        now: i64,
    ) -> Result<ChatOrganizationResponse> {
        request.validate()?;
        owner_only(principal, "read organization")?;
        self.ensure_committed_reads()?;
        let conn = &self.connection;
        let watermark = event_high_water(conn)?;
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
        // The visibility rule of the default `agent.list` rows, so a row
        // lists exactly where the projection counts it.
        let visible;
        let archived: &str = match request.archived {
            ArchivedFilter::Exclude => {
                visible = default_visible(conn)?;
                &visible
            }
            ArchivedFilter::Only => "a.archived = 1",
            ArchivedFilter::Include => "1 = 1",
        };
        let read = GroupRead {
            archived,
            limits,
            watermark,
            now,
            query: request.query.as_deref(),
        };
        if let Some(project) = request.project.as_deref() {
            return self.single_project_organization(
                conn,
                project,
                request.after.as_deref(),
                &read,
            );
        }
        self.all_projects_organization(conn, request.after.as_deref(), &read)
    }

    /// One project group (or an empty page past the cursor) with its exact
    /// total from the same scoped predicate the member page uses.
    fn single_project_organization(
        &self,
        conn: &Connection,
        project: &str,
        after: Option<&str>,
        read: &GroupRead<'_>,
    ) -> Result<ChatOrganizationResponse> {
        let mut counts = BTreeMap::new();
        let mut order = Vec::new();
        // The cursor is positional: a project sorts at or before it when
        // the page is already past.
        if after.is_none_or(|after| after < project) {
            counts.insert(
                Some(project.to_owned()),
                organization_count(conn, Some(project), read)?,
            );
            order.push(Some(project.to_owned()));
        }
        self.organization_page(
            conn,
            OrganizationPage {
                order,
                counts: &counts,
                read,
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
        read: &GroupRead<'_>,
    ) -> Result<ChatOrganizationResponse> {
        let archived = read.archived;
        let mut keys = overview::list_overview_projects(conn, None, after)?;
        let groups_truncated = keys.len() > MAX_PAGE_ENTRIES;
        if groups_truncated {
            keys.truncate(MAX_PAGE_ENTRIES);
        }
        // Exact per-group totals without materializing rows: the key
        // expression mirrors the member queries below. Chats group by
        // their overlay link alone; a retained execution label never
        // places a chat. The search predicate matches the member pages.
        let mut counts: BTreeMap<Option<String>, usize> = BTreeMap::new();
        {
            let mut totals = conn
                .prepare(&format!(
                    "SELECT {}, COUNT(*)
                     FROM agents a LEFT JOIN chat_organization o ON o.agent = a.id
                     WHERE {archived}{} GROUP BY 1",
                    organization_group(),
                    read.query_suffix(1),
                ))
                .map_err(sql_error)?;
            let rows = totals
                .query_map(
                    rusqlite::params_from_iter(read.query_params().iter()),
                    |row| Ok((row.get::<_, Option<String>>(0)?, row.get::<_, i64>(1)?)),
                )
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
                read,
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
            event_watermark: page.read.watermark,
            as_of: page.read.now,
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
                        page.read,
                    )?);
                }
                continue;
            }
            let take = full.min(MAX_ORG_MEMBERS_PER_GROUP).min(budget);
            budget -= take;
            kept += take;
            let mut members = Vec::with_capacity(take);
            organization_members(conn, key.as_deref(), page.read, take, &mut members)?;
            response.groups.push(self.organization_group(
                conn,
                key,
                members,
                full,
                full > take,
                page.read,
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
        read: &GroupRead<'_>,
    ) -> Result<OrgGroupView> {
        let archived = read.archived;
        let transfer = match project.as_deref() {
            Some(project) if projects::project_registration_tx(conn, project)?.is_some() => {
                project_transfer::open_intent(conn, project)?
            }
            _ => None,
        };
        let (registration, assignment, manager, needs_assignment, conflicting_roots, team) =
            match project.as_deref() {
                Some(project) => {
                    let registration = projects::project_registration_tx(conn, project)?;
                    if registration.is_none() {
                        // Execution labels and assignments without a retained
                        // registration group by label but carry no team detail.
                        (None, None, None, false, Vec::new(), None)
                    } else {
                        let assignment = project_assignment::read_assignment(conn, project)?;
                        let manager = overview::overview_manager_view(conn, Some(&assignment))?;
                        let mut needs_assignment = assignment.manager.is_none()
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
                        let conflicting_roots = match manager_num {
                            Some(manager) => organization_roots(conn, project, "1 = 1")?
                                .into_iter()
                                .filter(|root| *root != manager)
                                .map(format_agent_id)
                                .collect(),
                            None => {
                                let writers = retained_root_writers(
                                    conn,
                                    project,
                                    None,
                                    MAX_ORG_ROOTS_PER_GROUP as i64,
                                )?;
                                needs_assignment |= !writers.is_empty();
                                if writers.len() > 1 {
                                    writers.into_iter().map(format_agent_id).collect()
                                } else {
                                    Vec::new()
                                }
                            }
                        };
                        (
                            registration,
                            Some(assignment),
                            manager,
                            needs_assignment,
                            conflicting_roots,
                            Some(overview::project_header_totals(conn, project, read.limits)?),
                        )
                    }
                }
                None => (None, None, None, false, Vec::new(), None),
            };
        let (totals, capacity) = team.unzip();
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
            totals,
            capacity,
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
        let expected = request.expected_revision().clone();
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
            let mut rows = Vec::with_capacity(nums.len());
            for (index, (agent, num)) in nums.into_iter().enumerate() {
                let current: Option<(Option<String>, String, i64, Option<String>)> = tx
                    .query_row(
                        "SELECT project_link, tags, revision, continued_from
                         FROM chat_organization WHERE agent = ?1",
                        [num],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                    )
                    .optional()
                    .map_err(sql_error)?;
                // A menu write carries the revision it observed (0 for no
                // row yet); a write that raced another is refused whole and
                // nothing is applied.
                let observed = current.as_ref().map_or(0, |(_, _, revision, _)| *revision);
                if expected
                    .as_ref()
                    .is_some_and(|entries| entries[index] != observed)
                {
                    return Err(Error::new(
                        "conflict",
                        format!("Agent {agent} was reorganized since it was read; read it again"),
                    ));
                }
                let revision = observed + 1;
                // Attach/detach/tag never write provenance: the upsert below
                // leaves `continued_from` as recorded, so a continuation
                // survives every placement and tag change.
                let (current_link, current_tags, continued_from) = match current {
                    Some((link, tags, _, continued_from)) => (link, tags, continued_from),
                    None => (None, "[]".to_owned(), None),
                };
                let (link, tags) = match &change {
                    LinkChange::Attach { project } => (Some(project.clone()), current_tags),
                    LinkChange::Detach => (None, current_tags),
                    LinkChange::Tag { tags } => (current_link, encode_json(tags)?),
                };
                db!(tx.execute(
                    "INSERT INTO chat_organization(agent, project_link, tags, revision)
                     VALUES (?1, ?2, ?3, ?4)
                     ON CONFLICT(agent) DO UPDATE SET project_link = excluded.project_link,
                         tags = excluded.tags, revision = excluded.revision",
                    params![num, link, tags, revision],
                ));
                // The event row is the Agent's `agent.list` placement as
                // this write left it, read through the same group key.
                let (organization, row_tags, _) = agent_organization(&tx, num)?;
                rows.push(OrgRowChange {
                    agent: agent.clone(),
                    organization,
                    tags: row_tags,
                    revision,
                });
                links.push(OrgLinkView {
                    agent,
                    project_link: link,
                    tags: decode_tags(&tags)?,
                    revision,
                    continued_from,
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
            // transaction; the router publishes it after commit. Its rows
            // let a client refresh the Agents it already loaded by id.
            let change = OrganizationChangedEvent {
                id: id.to_owned(),
                method: method.to_owned(),
                rows,
            };
            change.validate()?;
            let event = insert_daemon_event(
                &tx,
                now,
                ORGANIZATION_CHANGED_KIND,
                "organization",
                None,
                None,
                serde_json::to_value(&change)
                    .map_err(|_| Error::new("internal", "cannot encode organization event"))?,
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
