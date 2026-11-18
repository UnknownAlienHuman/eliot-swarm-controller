//! Headless chat and work organization views and mutations (step 4a).
//!
//! Organization is placement, not authority. Attach/detach move a chat's
//! display placement between a retained project and the no-project bucket;
//! tags group Agents and chats; rename stays the desktop `[desktop]
//! display_names` presentation preference and archive stays `agent.archive`.
//! None of these change execution authority, ownership, files or attribution:
//! the execution project, scope, workspace and history of every Agent keep
//! their retained values: attach/detach/tag never start execution and
//! never edit history.

use super::{
    agent_lifecycle::ArchivedFilter, overview::OverviewManagerView,
    project_assignment::ProjectAssignment,
};
use crate::{AgentState, Error, Result, parse_agent_id};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Most Agents one attach/detach/tag call may carry. Mirrors the handoff
/// selection bound: one call stays one bounded page of rows.
pub const MAX_ORG_AGENTS: usize = 64;
/// Most tags one tag call may set on each Agent.
pub const MAX_ORG_TAGS: usize = 16;
/// Most bytes of one tag.
pub const MAX_ORG_TAG_BYTES: usize = 64;
/// Most members read back in one organization group.
pub const MAX_ORG_MEMBERS_PER_GROUP: usize = 200;
/// Most members read back across all groups of one projection.
pub const MAX_ORG_MEMBERS_TOTAL: usize = 1_000;

fn validate_mutation_id(id: &str) -> Result<()> {
    if id.trim().is_empty() || id.len() > 256 {
        return Err(Error::new(
            "invalid",
            "mutation id must be a nonblank string of at most 256 bytes",
        ));
    }
    Ok(())
}

fn validate_agents(agents: &[String]) -> Result<()> {
    if agents.is_empty() || agents.len() > MAX_ORG_AGENTS {
        return Err(Error::new(
            "invalid",
            "organization selection carries 1 to 64 Agents",
        ));
    }
    let mut seen = std::collections::HashSet::new();
    for agent in agents {
        parse_agent_id(agent)?;
        if !seen.insert(agent) {
            return Err(Error::new(
                "invalid",
                "organization selection must not repeat an Agent",
            ));
        }
    }
    Ok(())
}

fn validate_project_name(value: &str, field: &str) -> Result<()> {
    if value.is_empty() || value.len() > 1024 || value.contains(char::is_control) {
        return Err(Error::invalid_field(
            field,
            "nonempty name of at most 1024 bytes",
        ));
    }
    Ok(())
}

fn validate_tags(tags: &[String]) -> Result<()> {
    if tags.len() > MAX_ORG_TAGS {
        return Err(Error::new(
            "invalid",
            "organization tags carry at most 16 entries",
        ));
    }
    let mut seen = std::collections::HashSet::new();
    for tag in tags {
        if tag.is_empty()
            || tag.len() > MAX_ORG_TAG_BYTES
            || tag.trim() != tag
            || tag.contains(char::is_control)
        {
            return Err(Error::new(
                "invalid",
                "organization tags are trimmed nonempty strings of at most 64 bytes",
            ));
        }
        if !seen.insert(tag) {
            return Err(Error::new("invalid", "organization tags must not repeat"));
        }
    }
    Ok(())
}

/// One Agent's organizational link readback: placement and tags. Change
/// readback rides the projection's event watermark, not a revision.
#[derive(Clone, Debug, PartialEq, Eq, JsonSchema, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrgLinkView {
    pub agent: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_link: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
}

impl OrgLinkView {
    pub fn validate(&self) -> Result<()> {
        parse_agent_id(&self.agent)?;
        if let Some(project) = &self.project_link {
            validate_project_name(project, "organization link project")?;
        }
        validate_tags(&self.tags)
    }
}

/// One member of an organization group: execution facts plus the overlay.
#[derive(Clone, Debug, PartialEq, Eq, JsonSchema, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrgMemberView {
    pub agent: String,
    pub generation: i64,
    pub state: AgentState,
    pub harness: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    pub root: String,
    /// The retained execution project label (empty for no-project chats).
    pub project: String,
    /// The retained execution directory, independently of placement.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<String>,
    #[serde(default)]
    pub chat: bool,
    #[serde(default)]
    pub archived: bool,
    /// Placement override for chats; execution governs every other Agent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_link: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    /// True when this row is grouped by its placement link, not execution.
    #[serde(default)]
    pub placed: bool,
}

impl OrgMemberView {
    pub fn validate(&self) -> Result<()> {
        parse_agent_id(&self.agent)?;
        if self.generation < 0 {
            return Err(Error::new("invalid", "member generation is invalid"));
        }
        if self.harness.trim().is_empty() {
            return Err(Error::new("invalid", "member harness is invalid"));
        }
        if let Some(parent) = &self.parent {
            parse_agent_id(parent)?;
        }
        parse_agent_id(&self.root)?;
        if !self.chat && self.project.trim().is_empty() {
            return Err(Error::new(
                "unknown",
                "organization response carries a projectless Agent",
            ));
        }
        if self.chat && !self.project.is_empty() {
            return Err(Error::new(
                "unknown",
                "organization response carries a project chat",
            ));
        }
        if let Some(link) = &self.project_link {
            validate_project_name(link, "organization response")?;
            if let Some(workspace) = &self.workspace
                && (workspace.is_empty()
                    || workspace.len() > 4096
                    || workspace.contains(char::is_control))
            {
                return Err(Error::new(
                    "unknown",
                    "organization response workspace is invalid",
                ));
            }
        }
        validate_tags(&self.tags)?;
        Ok(())
    }
}

/// One organization group: a retained project team, or the unplaced bucket
/// (`project` absent) for chats without placement and Agents whose
/// execution project is not a retained registration.
#[derive(Clone, Debug, PartialEq, Eq, JsonSchema, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrgGroupView {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub registration: Option<super::agent_views::ProjectRegistrationView>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assignment: Option<ProjectAssignment>,
    /// True when the project has no manager but has root members waiting:
    /// shown as needing assignment, never auto-assigned.
    #[serde(default)]
    pub needs_assignment: bool,
    /// Independent roots beyond the first, by Agent id: competing execution
    /// roots surface here without reassigning any of them. Counts every
    /// retained root independently of the archived display filter.
    #[serde(default)]
    pub conflicting_roots: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manager: Option<OverviewManagerView>,
    #[serde(default)]
    pub members: Vec<OrgMemberView>,
    pub member_total: usize,
    #[serde(default)]
    pub members_truncated: bool,
}

impl OrgGroupView {
    pub fn validate(&self) -> Result<()> {
        if let Some(project) = &self.project {
            validate_project_name(project, "organization response")?;
        }
        if let Some(registration) = &self.registration {
            registration.validate().map_err(|_| {
                Error::new("unknown", "organization response registration is invalid")
            })?;
            if self.project.as_deref() != Some(registration.id.as_str()) {
                return Err(Error::new(
                    "unknown",
                    "organization group registration differs",
                ));
            }
        } else if self.assignment.is_some() || self.manager.is_some() {
            return Err(Error::new(
                "unknown",
                "organization group carries team detail without a registration",
            ));
        }
        if let Some(assignment) = &self.assignment
            && self.project.as_deref() != Some(assignment.project.as_str())
        {
            return Err(Error::new(
                "unknown",
                "organization group assignment differs",
            ));
        }
        if let Some(manager) = &self.manager {
            manager
                .validate()
                .map_err(|_| Error::new("unknown", "organization response manager is invalid"))?;
        }
        {
            let mut seen = std::collections::HashSet::new();
            for root in &self.conflicting_roots {
                parse_agent_id(root).map_err(|_| {
                    Error::new("unknown", "organization conflicting root is invalid")
                })?;
                if !seen.insert(root) {
                    return Err(Error::new(
                        "unknown",
                        "organization conflicting roots repeat",
                    ));
                }
            }
        }
        for member in &self.members {
            member.validate()?;
        }
        if self.member_total < self.members.len() || self.members.len() > MAX_ORG_MEMBERS_PER_GROUP
        {
            return Err(Error::new(
                "unknown",
                "organization group member bound differs",
            ));
        }
        Ok(())
    }
}

/// `chat.organization`: the grouped work projection. Groups follow project
/// assignment, manager, repository/worktree and ancestry; the caller gets a
/// ready projection with event-watermark readback, so no client repeats
/// Git discovery or grouping.
#[derive(Clone, Debug, Default, JsonSchema, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChatOrganizationRequest {
    /// Read back one registered project only; omitted reads every group.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    /// Archived rows stay hidden unless included or selected alone.
    #[serde(default)]
    pub archived: ArchivedFilter,
}

impl ChatOrganizationRequest {
    pub fn validate(&self) -> Result<()> {
        if let Some(project) = &self.project {
            validate_project_name(project, "params.project")?;
        }
        Ok(())
    }

    pub fn validate_response(&self, reply: &ChatOrganizationResponse) -> Result<()> {
        reply.validate()?;
        if let Some(project) = &self.project
            && (reply
                .groups
                .iter()
                .any(|group| group.project.as_deref() != Some(project))
                || reply.groups.is_empty())
        {
            return Err(Error::new(
                "unknown",
                "organization response differs from its project filter",
            ));
        }
        Ok(())
    }
}

/// The grouped projection readback: every group plus the event watermark
/// the committed snapshot observed. There is no organization revision;
/// mutation readback replays the retained row.
#[derive(Clone, Debug, PartialEq, Eq, JsonSchema, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChatOrganizationResponse {
    /// Store event sequence at the committed snapshot used for this reply.
    #[serde(default)]
    pub event_watermark: i64,
    pub as_of: i64,
    #[serde(default)]
    pub groups: Vec<OrgGroupView>,
    pub member_total: usize,
    #[serde(default)]
    pub truncated: bool,
}

impl ChatOrganizationResponse {
    pub fn validate(&self) -> Result<()> {
        if self.event_watermark < 0 {
            return Err(Error::new(
                "unknown",
                "organization response watermark is invalid",
            ));
        }
        let mut previous: Option<&str> = None;
        let mut members = 0_usize;
        for group in &self.groups {
            group.validate()?;
            // Registered projects sort first; the unplaced bucket sorts last.
            let key = group.project.as_deref().unwrap_or("\u{10ffff}");
            if previous.is_some_and(|prev| key <= prev) {
                return Err(Error::new("unknown", "organization groups are not ordered"));
            }
            previous = Some(key);
            members += group.members.len();
            if group.members_truncated && group.member_total <= group.members.len() {
                return Err(Error::new(
                    "unknown",
                    "organization group truncation differs",
                ));
            }
        }
        if members > MAX_ORG_MEMBERS_TOTAL
            || self.member_total < members
            || (self.truncated && self.member_total <= members)
        {
            return Err(Error::new(
                "unknown",
                "organization response member bound differs",
            ));
        }
        Ok(())
    }
}

/// `chat.attach`: place chats under a retained project. Placement only:
/// execution project, scope, workspace and history are untouched.
#[derive(Clone, Debug, JsonSchema, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChatAttachRequest {
    pub id: String,
    pub agents: Vec<String>,
    pub project: String,
}

impl ChatAttachRequest {
    pub fn validate(&self) -> Result<()> {
        validate_mutation_id(&self.id)?;
        validate_agents(&self.agents)?;
        validate_project_name(&self.project, "params.project")
    }

    pub fn validate_response(&self, reply: &ChatOrganizationMutationResponse) -> Result<()> {
        reply.validate()?;
        if reply.id != self.id
            || reply.links.len() != self.agents.len()
            || reply.links.iter().zip(&self.agents).any(|(link, agent)| {
                &link.agent != agent || link.project_link.as_deref() != Some(&self.project)
            })
        {
            return Err(Error::new("unknown", "attach response binding differs"));
        }
        Ok(())
    }
}

/// `chat.detach`: return chats to the no-project bucket. Placement only.
#[derive(Clone, Debug, JsonSchema, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChatDetachRequest {
    pub id: String,
    pub agents: Vec<String>,
}

impl ChatDetachRequest {
    pub fn validate(&self) -> Result<()> {
        validate_mutation_id(&self.id)?;
        validate_agents(&self.agents)
    }

    pub fn validate_response(&self, reply: &ChatOrganizationMutationResponse) -> Result<()> {
        reply.validate()?;
        if reply.id != self.id
            || reply.links.len() != self.agents.len()
            || reply
                .links
                .iter()
                .zip(&self.agents)
                .any(|(link, agent)| &link.agent != agent || link.project_link.is_some())
        {
            return Err(Error::new("unknown", "detach response binding differs"));
        }
        Ok(())
    }
}

/// `chat.tag`: replace the tag groups of Agents or chats.
#[derive(Clone, Debug, JsonSchema, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChatTagRequest {
    pub id: String,
    pub agents: Vec<String>,
    #[serde(default)]
    pub tags: Vec<String>,
}

impl ChatTagRequest {
    pub fn validate(&self) -> Result<()> {
        validate_mutation_id(&self.id)?;
        validate_agents(&self.agents)?;
        validate_tags(&self.tags)
    }

    pub fn validate_response(&self, reply: &ChatOrganizationMutationResponse) -> Result<()> {
        reply.validate()?;
        if reply.id != self.id
            || reply.links.len() != self.agents.len()
            || reply
                .links
                .iter()
                .zip(&self.agents)
                .any(|(link, agent)| &link.agent != agent || link.tags != self.tags)
        {
            return Err(Error::new("unknown", "tag response binding differs"));
        }
        Ok(())
    }
}

/// The attach/detach/tag readback: one link row per selected Agent, in
/// request order. Change readback rides the projection's event watermark.
#[derive(Clone, Debug, PartialEq, Eq, JsonSchema, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChatOrganizationMutationResponse {
    pub id: String,
    pub links: Vec<OrgLinkView>,
}

impl ChatOrganizationMutationResponse {
    pub fn validate(&self) -> Result<()> {
        validate_mutation_id(&self.id)?;
        if self.links.is_empty() || self.links.len() > MAX_ORG_AGENTS {
            return Err(Error::new(
                "unknown",
                "organization mutation link bound differs",
            ));
        }
        for link in &self.links {
            link.validate()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn agent() -> String {
        "a1".to_owned()
    }

    #[test]
    fn attach_round_trips_and_rejects_bad_shapes() {
        let request = ChatAttachRequest {
            id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            agents: vec![agent()],
            project: "app".to_owned(),
        };
        request.validate().unwrap();
        let reply = ChatOrganizationMutationResponse {
            id: request.id.clone(),
            links: vec![OrgLinkView {
                agent: agent(),
                project_link: Some("app".to_owned()),
                tags: Vec::new(),
            }],
        };
        request.validate_response(&reply).unwrap();
        let wrong = ChatOrganizationMutationResponse {
            id: request.id.clone(),
            links: Vec::new(),
        };
        assert!(wrong.validate().is_err());
        let mut detached = reply.clone();
        detached.links[0].project_link = None;
        assert!(request.validate_response(&detached).is_err());
        let bad: ChatAttachRequest = serde_json::from_value(json!({
            "id": "x",
            "agents": ["a1", "a1"],
            "project": "app",
        }))
        .unwrap();
        assert!(bad.validate().is_err());
        let bad_project: ChatAttachRequest = serde_json::from_value(json!({
            "id": "x",
            "agents": ["a1"],
            "project": "",
        }))
        .unwrap();
        assert!(bad_project.validate().is_err());
        assert!(serde_json::from_value::<ChatAttachRequest>(json!({"id": "x"})).is_err());
    }

    #[test]
    fn tag_rejects_repeats_and_overlong_entries() {
        let request = ChatTagRequest {
            id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            agents: vec![agent()],
            tags: vec!["inbox".to_owned()],
        };
        request.validate().unwrap();
        let reply = ChatOrganizationMutationResponse {
            id: request.id.clone(),
            links: vec![OrgLinkView {
                agent: agent(),
                project_link: None,
                tags: vec!["inbox".to_owned()],
            }],
        };
        request.validate_response(&reply).unwrap();
        for tags in [
            vec!["a".to_owned(), "a".to_owned()],
            vec![" x".to_owned()],
            vec!["x".repeat(MAX_ORG_TAG_BYTES + 1)],
        ] {
            let bad = ChatTagRequest {
                id: request.id.clone(),
                agents: vec![agent()],
                tags,
            };
            assert!(bad.validate().is_err());
        }
    }

    #[test]
    fn organization_orders_projects_before_the_unplaced_bucket() {
        let request = ChatOrganizationRequest {
            project: None,
            archived: ArchivedFilter::Exclude,
        };
        request.validate().unwrap();
        let member = OrgMemberView {
            agent: agent(),
            generation: 1,
            state: AgentState::Idle,
            harness: "codex".to_owned(),
            parent: None,
            root: agent(),
            project: String::new(),
            workspace: None,
            chat: true,
            archived: false,
            project_link: None,
            tags: Vec::new(),
            placed: false,
        };
        let reply = ChatOrganizationResponse {
            event_watermark: 7,
            as_of: 7,
            groups: vec![OrgGroupView {
                project: None,
                registration: None,
                assignment: None,
                needs_assignment: false,
                conflicting_roots: Vec::new(),
                manager: None,
                members: vec![member],
                member_total: 1,
                members_truncated: false,
            }],
            member_total: 1,
            truncated: false,
        };
        request.validate_response(&reply).unwrap();
        let filtered = ChatOrganizationRequest {
            project: Some("app".to_owned()),
            archived: ArchivedFilter::Exclude,
        };
        assert!(filtered.validate_response(&reply).is_err());
    }
}
