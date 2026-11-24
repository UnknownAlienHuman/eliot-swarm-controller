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

fn organization_watermark(conn: &Connection) -> Result<i64> {
    conn.query_row(
        "SELECT MAX(COALESCE((SELECT MAX(seq) FROM events), 0),
                COALESCE((SELECT seq FROM sqlite_sequence WHERE name = 'events'), 0))",
        [],
        |row| row.get(0),
    )
    .map_err(sql_error)
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
/// A legacy Agent whose retained project label is a checkout path groups
/// under the registration of that exact path, without rewriting the
/// retained label; two registrations on one path stay unplaced (ambiguous).
pub(super) fn organization_group() -> String {
    let path = checkout_key_sql("a.project");
    let checkout = checkout_key_sql("r.checkout");
    format!(
        "CASE WHEN (a.chat = 1 AND o.project_link IS NOT NULL
         AND EXISTS(SELECT 1 FROM project_registrations r WHERE r.id = o.project_link))
    THEN o.project_link
    WHEN (a.chat = 0
          AND EXISTS(SELECT 1 FROM project_registrations r WHERE r.id = a.project))
    THEN a.project
    WHEN (a.chat = 0 AND (SELECT COUNT(*) FROM project_registrations r
          WHERE {checkout} = {path}) = 1)
    THEN (SELECT r.id FROM project_registrations r WHERE {checkout} = {path})
    ELSE NULL END"
    )
}

/// One normalized checkout path, trailing separators trimmed. On Windows a
/// `\\?\` verbatim prefix is dropped, backslashes become `/` and ASCII case
/// is folded (Windows compares paths case-insensitively; SQLite's `lower`
/// folds ASCII only). Elsewhere case and a literal backslash are part of the
/// name, so `/repo/Foo` and `/repo/foo` stay distinct. [`checkout_key_sql`]
/// is its SQL twin.
fn checkout_key(path: &str) -> String {
    if cfg!(windows) {
        path.replace(r"\\?\", "")
            .replace('\\', "/")
            .trim_end_matches('/')
            .to_ascii_lowercase()
    } else {
        path.trim_end_matches('/').to_owned()
    }
}

/// The SQL twin of [`checkout_key`] over one column expression: the group
/// key and both member predicates normalize through it.
fn checkout_key_sql(column: &str) -> String {
    if cfg!(windows) {
        format!(r"lower(rtrim(replace(replace({column}, '\\?\', ''), '\', '/'), '/'))")
    } else {
        format!("rtrim({column}, '/')")
    }
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

/// Project-scoped member predicate shared by the count and the page, equal
/// to [`organization_group`] `= ?1`: members of a retained registration
/// only; chats by their overlay link alone (a retained execution label
/// never places a chat), every other Agent by its execution project, or by
/// the registration of its exact legacy checkout path when the requested
/// group's checkout owns that path uniquely. `?2` binds the group's
/// normalized checkout key (empty without a registration, matching
/// nothing: labels are nonblank).
fn organization_project_members() -> String {
    let path = checkout_key_sql("a.project");
    format!(
        "(EXISTS(SELECT 1 FROM project_registrations r WHERE r.id = ?1)
      AND ((a.chat = 0 AND (a.project = ?1
           OR (?2 <> '' AND {path} = ?2
               AND (SELECT COUNT(*) FROM project_registrations r2
                    WHERE {} = ?2) = 1)))
        OR (a.chat = 1 AND o.project_link = ?1)))",
        checkout_key_sql("r2.checkout")
    )
}

/// Unplaced-bucket predicate shared by the count and the page, equal to
/// [`organization_group`] `IS NULL`: chats without a retained link (a chat
/// whose linked project was removed included) plus non-chat Agents whose
/// execution project is neither a retained registration nor the exact
/// checkout path of exactly one registration.
fn organization_unplaced_members() -> String {
    let path = checkout_key_sql("a.project");
    format!(
        "((a.chat = 1 AND (o.project_link IS NULL
        OR NOT EXISTS(SELECT 1 FROM project_registrations r WHERE r.id = o.project_link)))
      OR (a.chat = 0 AND NOT EXISTS(SELECT 1 FROM project_registrations r WHERE r.id = a.project)
          AND (SELECT COUNT(*) FROM project_registrations r2
               WHERE {} = {path}) <> 1))",
        checkout_key_sql("r2.checkout")
    )
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
            let key = group_checkout_key(conn, name)?;
            let mut statement = conn
                .prepare(&sql(&organization_project_members(), 3))
                .map_err(sql_error)?;
            let mut params: Vec<rusqlite::types::Value> = vec![name.to_owned().into(), key.into()];
            params.extend(read.query_params());
            statement
                .query_row(rusqlite::params_from_iter(params.iter()), |row| row.get(0))
                .map_err(sql_error)?
        }
        None => {
            let mut statement = conn
                .prepare(&sql(&organization_unplaced_members(), 1))
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

/// The normalized checkout key of one group's registration, empty when the
/// label has no retained registration (the legacy path arm then matches
/// nothing).
fn group_checkout_key(conn: &Connection, project: &str) -> Result<String> {
    Ok(projects::project_registration_tx(conn, project)?
        .map(|registration| checkout_key(&registration.checkout))
        .unwrap_or_default())
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
    let project_predicate = organization_project_members();
    let unplaced_predicate = organization_unplaced_members();
    let mut statement = match project {
        Some(_) => conn
            .prepare(&format!(
                "SELECT a.id, a.generation, a.state, a.harness, a.parent, a.root, a.project,
                        a.chat, a.archived, a.observe_only, a.workspace,
                        o.project_link, o.tags, o.continued_from
                 FROM agents a LEFT JOIN chat_organization o ON o.agent = a.id
                 WHERE {project_predicate}
                   AND {archived}{}
                 ORDER BY a.id LIMIT ?3",
                read.query_suffix(4)
            ))
            .map_err(sql_error)?,
        None => conn
            .prepare(&format!(
                "SELECT a.id, a.generation, a.state, a.harness, a.parent, a.root, a.project,
                        a.chat, a.archived, a.observe_only, a.workspace,
                        o.project_link, o.tags, o.continued_from
                 FROM agents a LEFT JOIN chat_organization o ON o.agent = a.id
                 WHERE {unplaced_predicate}
                   AND {archived}{}
                 ORDER BY a.id LIMIT ?1",
                read.query_suffix(2)
            ))
            .map_err(sql_error)?,
    };
    // `?1` name, `?2` checkout key, `?3` page bound, search words from `?4`;
    // the unplaced bucket binds `?1` page bound, words from `?2`.
    let mut params: Vec<rusqlite::types::Value> = match project {
        Some(name) => vec![
            name.to_owned().into(),
            group_checkout_key(conn, name)?.into(),
            take.into(),
        ],
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
                        // Competing roots need retained evidence of a
                        // competing manager: the assignment names one, and
                        // every other retained root competes with it.
                        // Without a manager the roots wait for assignment,
                        // never a conflict.
                        let conflicting_roots = match manager_num {
                            Some(manager) => organization_roots(conn, project, "1 = 1")?
                                .into_iter()
                                .filter(|root| *root != manager)
                                .map(format_agent_id)
                                .collect(),
                            None => Vec::new(),
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
