use super::*;
use eliot_core::api::{ProjectOverviewRequest, ProjectOverviewResponse};

// Keep room for the RPC envelope and for MCP's JSON string representation.
const PAGE_BYTES: usize = 1536 * 1024;
const CURSOR_BYTES: usize = 8192;

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(tag = "section", rename_all = "snake_case", deny_unknown_fields)]
enum Cursor {
    Projects {
        after: String,
    },
    Team {
        project: String,
        offsets: TeamOffsets,
    },
    Native {
        after: i64,
    },
}

#[derive(Clone, Copy, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct TeamOffsets {
    workers: Option<i64>,
    workspaces: Option<i64>,
}

impl TeamOffsets {
    const START: Self = Self {
        workers: Some(0),
        workspaces: Some(0),
    };
}

impl Cursor {
    fn decode(value: &str, filter: Option<&str>) -> Result<Self> {
        let invalid = || Error::invalid_field("params.after", "project overview continuation");
        let cursor: Self = serde_json::from_str(value).map_err(|_| invalid())?;
        let valid_project = |project: &str| {
            !project.is_empty()
                && project.len() <= 1024
                && !project.contains(char::is_control)
                && filter.is_none_or(|filter| filter == project)
        };
        let valid = match &cursor {
            Self::Projects { after } => valid_project(after),
            Self::Team { project, offsets } => {
                valid_project(project)
                    && offsets.workers.or(offsets.workspaces).is_some()
                    && offsets.workers.is_none_or(|after| after >= 0)
                    && offsets.workspaces.is_none_or(|after| after >= 0)
            }
            Self::Native { after } => *after >= 0,
        };
        if !valid {
            return Err(invalid());
        }
        Ok(cursor)
    }
}

struct PageBudget(usize);

impl PageBudget {
    fn take(&mut self, item: &impl serde::Serialize) -> Result<bool> {
        let bytes = encode_json(item)?.len() + 1;
        if bytes > self.0 {
            return Ok(false);
        }
        self.0 -= bytes;
        Ok(true)
    }
}

impl Store {
    pub fn project_overview(
        &self,
        limits: &ScopeLimits,
        request: &ProjectOverviewRequest,
        principal: &str,
        as_of: i64,
    ) -> Result<ProjectOverviewResponse> {
        request.validate()?;
        let filter = request.project.as_deref();
        let cursor = request
            .after
            .as_deref()
            .map(|value| Cursor::decode(value, filter))
            .transpose()?;
        self.ensure_committed_reads()?;
        let tx = self.connection.unchecked_transaction().map_err(sql_error)?;
        let access = DetailAccess::resolve(&tx, principal)?;
        if let Some(project) = filter
            && list_overview_projects(&tx, Some(project), None)?.is_empty()
        {
            return Err(Error::new(
                "not_found",
                format!("project {project} was not found"),
            ));
        }
        let (fleet, coverage) = read_fleet(&tx, limits)?;
        let event_watermark = tx
            .query_row(
                "SELECT MAX(COALESCE((SELECT MAX(seq) FROM events), 0),
                        COALESCE((SELECT seq FROM sqlite_sequence WHERE name = 'events'), 0))",
                [],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        let mut response = ProjectOverviewResponse {
            as_of,
            event_watermark,
            coverage,
            page_complete: true,
            next_cursor: None,
            fleet,
            projects: Vec::new(),
        };
        let budget = (PAGE_BYTES - CURSOR_BYTES)
            .checked_sub(encode_json(&response)?.len())
            .ok_or_else(|| {
                Error::new(
                    "frame_too_large",
                    "fleet totals exceed the overview page budget",
                )
            })?;
        let mut budget = PageBudget(budget);
        let continuation = match cursor {
            Some(Cursor::Native { after }) => page_native(
                &tx,
                filter,
                &access,
                after,
                &mut budget,
                &mut response.fleet,
            )?,
            cursor => {
                let project_continuation = page_projects(
                    &tx,
                    filter,
                    &access,
                    limits,
                    cursor,
                    &mut budget,
                    &mut response.projects,
                )?;
                match project_continuation {
                    Some(cursor) => Some(cursor),
                    None => page_native(&tx, filter, &access, 0, &mut budget, &mut response.fleet)?,
                }
            }
        };
        response.next_cursor = continuation.as_ref().map(encode_json).transpose()?;
        response.page_complete = continuation.is_none();
        request.validate_response(&response)?;
        tx.commit().map_err(sql_error)?;
        Ok(response)
    }
}

fn page_projects(
    conn: &Connection,
    filter: Option<&str>,
    access: &DetailAccess,
    limits: &ScopeLimits,
    cursor: Option<Cursor>,
    budget: &mut PageBudget,
    page: &mut Vec<ProjectOverviewEntry>,
) -> Result<Option<Cursor>> {
    let after = match cursor.as_ref() {
        Some(Cursor::Projects { after }) => Some(after.as_str()),
        Some(Cursor::Team { project, offsets }) => {
            if !list_overview_projects(conn, Some(project), None)?.is_empty()
                && let Some(cursor) =
                    page_team(conn, project, limits, access, *offsets, budget, page)?
            {
                return Ok(Some(cursor));
            }
            Some(project.as_str())
        }
        _ => None,
    };
    let keys = list_overview_projects(conn, filter, after)?;
    for (index, project) in keys.into_iter().enumerate() {
        if index == MAX_PAGE_ENTRIES {
            return Ok(page.last().map(|last| Cursor::Projects {
                after: last.project.clone(),
            }));
        }
        if let Some(cursor) = page_team(
            conn,
            &project,
            limits,
            access,
            TeamOffsets::START,
            budget,
            page,
        )? {
            return Ok(Some(cursor));
        }
    }
    Ok(None)
}

fn page_team(
    conn: &Connection,
    project: &str,
    limits: &ScopeLimits,
    access: &DetailAccess,
    offsets: TeamOffsets,
    budget: &mut PageBudget,
    page: &mut Vec<ProjectOverviewEntry>,
) -> Result<Option<Cursor>> {
    let mut team = project_team(
        conn,
        project,
        limits,
        access,
        offsets.workers,
        offsets.workspaces,
    )?;
    let workers = std::mem::take(&mut team.workers);
    let workspaces = std::mem::take(&mut team.workspaces);
    if !budget.take(&team)? {
        return if page.is_empty() {
            Err(Error::new(
                "frame_too_large",
                "project overview team header exceeds the page budget",
            ))
        } else {
            Ok(Some(Cursor::Team {
                project: project.to_owned(),
                offsets,
            }))
        };
    }
    team.workers_truncated |= append_rows(budget, &mut team.workers, workers)?;
    team.workspaces_truncated |= append_rows(budget, &mut team.workspaces, workspaces)?;
    let next = if team.workers_truncated || team.workspaces_truncated {
        let worker_after = last_agent_offset(
            team.workers.last().map(|row| row.agent.as_str()),
            offsets.workers,
        )?;
        let workspace_after = last_agent_offset(
            team.workspaces.last().map(|row| row.agent.as_str()),
            offsets.workspaces,
        )?;
        if page.is_empty() && team.workers.is_empty() && team.workspaces.is_empty() {
            return Err(Error::new(
                "frame_too_large",
                "project overview detail exceeds the page budget",
            ));
        }
        Some(Cursor::Team {
            project: project.to_owned(),
            offsets: TeamOffsets {
                workers: worker_after.filter(|_| team.workers_truncated),
                workspaces: workspace_after.filter(|_| team.workspaces_truncated),
            },
        })
    } else {
        None
    };
    page.push(team);
    Ok(next)
}

fn last_agent_offset(id: Option<&str>, previous: Option<i64>) -> Result<Option<i64>> {
    Ok(id.map(parse_agent_id).transpose()?.or(previous))
}

// Row cost includes its separator. Header serialization accounts for the
// brackets; changing a truncation flag to true can only shrink the header.
fn append_rows<T: serde::Serialize>(
    budget: &mut PageBudget,
    page: &mut Vec<T>,
    rows: Vec<T>,
) -> Result<bool> {
    for row in rows {
        if !budget.take(&row)? {
            return Ok(true);
        }
        page.push(row);
    }
    Ok(false)
}

fn page_native(
    conn: &Connection,
    filter: Option<&str>,
    access: &DetailAccess,
    after: i64,
    budget: &mut PageBudget,
    fleet: &mut FleetTotals,
) -> Result<Option<Cursor>> {
    let (rows, truncated) = list_native_children(conn, filter, access, after)?;
    fleet.native_children_truncated =
        append_rows(budget, &mut fleet.native_children, rows)? || truncated;
    if fleet.native_children_truncated {
        let after = last_agent_offset(
            fleet.native_children.last().map(|row| row.child.as_str()),
            Some(after),
        )?
        .unwrap_or_default();
        return Ok(Some(Cursor::Native { after }));
    }
    Ok(None)
}
