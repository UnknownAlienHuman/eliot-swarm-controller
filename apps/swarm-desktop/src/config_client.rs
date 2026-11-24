//! The one client module behind the Settings page.
//!
//! Every setting the desktop reads or writes goes through the daemon's
//! unified `settings.read` / `settings.save` (one store, revision-guarded,
//! with author history), including the desktop's own `[desktop]`
//! preferences ([`crate::settings`]). The remaining calls are the runtime
//! reads and file-owned APIs the settings document does not cover:
//! `profile.list` (launchability), `project.list` (resolved registrations),
//! `routing.preview`, `rule.list`, `script.list/read/save`, `timer.run`,
//! the rule worker's health (`daemon.status`),
//! `workspace.list` (managed worktrees) and the fleet totals for Usage
//! (`agent.list` counts and the `daemon.status` retention view).
//! Each runs the same request/response validators the CLI runs.

use std::path::Path;

use eliot_core::AgentState;
use eliot_core::api::harness::MAX_CATALOG_LIMIT;
use eliot_core::api::{
    AgentListRequest, ArchivedFilter, DaemonStatusRequest, DaemonStatusResponse,
    HarnessQuotaRequest, HarnessQuotaResponse, ManagedWorkspaceView, ModelsCatalogRequest,
    ModelsCatalogResponse, ProfileListRequest, ProfileListResponse, ProjectRegistrationView,
    RetentionView, RoutingPreviewRequest, RoutingPreviewResponse, RuleListEntry, RuleListRequest,
    RuleListResponse, ScriptListRequest, ScriptListResponse, ScriptReadRequest, ScriptReadResponse,
    ScriptSaveRequest, ScriptSaveResponse, SettingsReadResponse, SettingsSaveRequest,
    SettingsSaveResponse, TimerRunRequest, WorkspaceListRequest, WorkspaceListResponse,
    discovery::ProjectListRequest,
};
use eliot_core::{Command, Error, Result, RoutingDecision};
use serde_json::Value;

use crate::settings::{self, Preferences};

/// Write the `[desktop]` preferences and window size.
pub async fn save_preferences(
    data_dir: &Path,
    preferences: Preferences,
    window: Option<(u32, u32)>,
) -> Result<SettingsSaveResponse> {
    settings::save_preferences(data_dir, preferences, window).await
}

/// Every settings section, its revision and history; imports the legacy
/// per-user preferences file once when `[desktop]` is absent.
pub async fn read_settings(data_dir: &Path) -> Result<SettingsReadResponse> {
    settings::read(data_dir).await
}

/// Validate (dry run) or save complete settings sections.
pub async fn save_settings(
    data_dir: &Path,
    request: &SettingsSaveRequest,
) -> Result<SettingsSaveResponse> {
    settings::save(data_dir, request).await
}

/// Unregister projects (Remove project in the sidebar): the `projects`
/// settings section without their ids, through the revision-guarded
/// settings writer. Their folders, Agents and files stay.
pub async fn remove_projects(data_dir: &Path, ids: &[String]) -> Result<SettingsSaveResponse> {
    use eliot_core::api::{SettingsClientSource, SettingsSection};
    let read = settings::read(data_dir).await?;
    let mut projects = read
        .sections
        .get(&SettingsSection::Projects)
        .cloned()
        .unwrap_or_else(|| Value::Object(Default::default()));
    if let Some(map) = projects.as_object_mut() {
        for id in ids {
            map.remove(id);
        }
    }
    let request = SettingsSaveRequest {
        expected_revision: read.revision,
        sections: [(SettingsSection::Projects, projects)].into(),
        dry_run: false,
        client: SettingsClientSource::Desktop,
    };
    settings::save(data_dir, &request).await
}

/// Project registrations for the launch picker. `project.list` returns a bare
/// array (not an object), so it cannot pass the typed object boundary; the
/// rows get the same field validation instead.
pub async fn list_projects(data_dir: &Path) -> Result<Vec<ProjectRegistrationView>> {
    let params = ProjectListRequest {};
    params.validate()?;
    let value =
        eliot_daemon::client_call(data_dir, "project.list", Value::Object(Default::default()))
            .await?;
    let registrations: Vec<ProjectRegistrationView> = serde_json::from_value(value)
        .map_err(|_| Error::new("unknown", "project.list returned a malformed result"))?;
    for registration in &registrations {
        registration.validate()?;
    }
    Ok(registrations)
}

/// Pages of `workspace.list` read at most (1,000 rows each).
const WORKSPACE_PAGES: usize = 10;

/// Every managed worktree, page by page, with the daemon's total; `false`
/// when the page bound stopped the read before the end.
pub async fn managed_workspaces(
    data_dir: &Path,
) -> Result<(Vec<ManagedWorkspaceView>, usize, bool)> {
    let mut rows = Vec::new();
    let mut after: Option<String> = None;
    let mut total = 0;
    for _ in 0..WORKSPACE_PAGES {
        let params = WorkspaceListRequest {
            after: after.clone(),
            limit: 1_000,
        };
        params.validate()?;
        let page: WorkspaceListResponse =
            eliot_daemon::client_call_typed(data_dir, "workspace.list", &params).await?;
        params.validate_response(&page)?;
        total = page.total;
        let done = page.workspaces.is_empty() || !page.truncated;
        after = page.next.clone();
        rows.extend(page.workspaces);
        if done {
            return Ok((rows, total, true));
        }
    }
    Ok((rows, total, false))
}

/// Complete fleet totals for Settings › Usage: Agents working now or
/// starting, all Agents (archived included), and the retention view.
#[derive(Clone, Debug, PartialEq)]
pub struct FleetTotals {
    pub running: usize,
    pub agents: usize,
    pub retention: Option<RetentionView>,
}

/// Exact counts (`agent.list` `total` per state, one row each) and the
/// `daemon.status` retention view: never a sum over the rows a window
/// happened to load.
pub async fn fleet_totals(data_dir: &Path) -> Result<FleetTotals> {
    let count = |state: Option<AgentState>| {
        let params = AgentListRequest {
            archived: ArchivedFilter::Include,
            limit: 1,
            state,
            ..AgentListRequest::default()
        };
        async move {
            crate::client::list_agents(data_dir, &params)
                .await
                .map(|page| page.total)
        }
    };
    let mut running = 0;
    for state in [AgentState::Busy, AgentState::Starting, AgentState::Created] {
        running += count(Some(state)).await?;
    }
    let agents = count(None).await?;
    let request = DaemonStatusRequest::default();
    request.validate()?;
    let status: DaemonStatusResponse =
        eliot_daemon::client_call_typed(data_dir, "daemon.status", &request).await?;
    request.validate_response(&status)?;
    Ok(FleetTotals {
        running,
        agents,
        retention: status.retention,
    })
}

#[derive(Clone, Debug, Default)]
pub enum RuleWorker {
    #[default]
    Unreadable,
    Running,
    Starting,
    Down(String),
}

/// Rule worker health from the daemon's own task status.
pub async fn rule_worker(data_dir: &Path) -> Result<RuleWorker> {
    use eliot_core::api::WorkerTaskHealth;
    let request = DaemonStatusRequest::default();
    request.validate()?;
    let status: DaemonStatusResponse =
        eliot_daemon::client_call_typed(data_dir, "daemon.status", &request).await?;
    request.validate_response(&status)?;
    Ok(status
        .automation_tasks
        .iter()
        .find(|task| task.name == "rule_worker")
        .map_or(RuleWorker::Unreadable, |task| match task.health {
            WorkerTaskHealth::Running => RuleWorker::Running,
            WorkerTaskHealth::Starting => RuleWorker::Starting,
            WorkerTaskHealth::Stopped => RuleWorker::Down("stopped".to_owned()),
            WorkerTaskHealth::Failed => RuleWorker::Down(task.failure_code.as_deref().map_or_else(
                || "stopped after an error".to_owned(),
                |code| format!("stopped after an error ({code})"),
            )),
        }))
}

/// The decision routing would record for the next launch of `role`
/// (advisory: admission evaluates again).
pub async fn routing_preview(data_dir: &Path, role: Option<String>) -> Result<RoutingDecision> {
    let params = RoutingPreviewRequest { role };
    params.validate()?;
    let reply: RoutingPreviewResponse =
        eliot_daemon::client_call_typed(data_dir, "routing.preview", &params).await?;
    params.validate_response(&reply)?;
    Ok(reply.decision)
}

/// Safety bound on `models.catalog` pages one listing reads (100 models
/// each). The daemon's own bound is per response, not per listing, so this
/// only stops a misbehaving daemon from paging forever. Reaching it leaves
/// the returned `next` set: the listing is partial and says so.
const CATALOG_MAX_PAGES: usize = 100;

/// The unified model catalogue for the composer's picker: every installed
/// harness with its version, and the models read page by page until the
/// daemon reports the end (`next` absent). If its snapshot revision changes
/// during paging, restart the listing once; never mix pages from two revisions.
/// The daemon caches and coalesces the native reads.
pub async fn models_catalog(data_dir: &Path) -> Result<ModelsCatalogResponse> {
    match read_models_catalog(data_dir).await {
        Err(error)
            if error.code == "invalid"
                && error
                    .message
                    .contains("catalogue cursor is no longer in the listing") =>
        {
            read_models_catalog(data_dir).await
        }
        result => result,
    }
}

async fn read_models_catalog(data_dir: &Path) -> Result<ModelsCatalogResponse> {
    let mut request = ModelsCatalogRequest {
        query: None,
        after: None,
        limit: Some(MAX_CATALOG_LIMIT),
    };
    let mut whole: Option<ModelsCatalogResponse> = None;
    let mut next = None;
    for _ in 0..CATALOG_MAX_PAGES {
        request.validate()?;
        let page: ModelsCatalogResponse =
            eliot_daemon::client_call_typed(data_dir, "models.catalog", &request).await?;
        request.validate_response(&page)?;
        next = page.next.clone();
        match whole.as_mut() {
            Some(whole) => {
                whole.models.extend(page.models);
                // Every page repeats the aggregate's failed harnesses.
                whole.failures = page.failures;
                whole.stale |= page.stale;
            }
            None => whole = Some(page),
        }
        let Some(after) = next.clone() else {
            break;
        };
        request.after = Some(after);
    }
    let mut whole = whole.ok_or_else(|| Error::new("unknown", "no catalogue page was read"))?;
    // `next` is the last page's cursor: `None` for a complete listing, set
    // when the page bound stopped the read (a partial listing).
    whole.next = next;
    Ok(whole)
}

/// Provider-supported quota of one harness under its ad-hoc defaults.
pub async fn harness_quota(data_dir: &Path, harness: &str) -> Result<HarnessQuotaResponse> {
    let params = HarnessQuotaRequest {
        harness: harness.to_owned(),
        profile: None,
    };
    params.validate()?;
    let reply: HarnessQuotaResponse =
        eliot_daemon::client_call_typed(data_dir, "harness.quota", &params).await?;
    params.validate_response(&reply)?;
    Ok(reply)
}

/// Sanitized configured-profile summaries for the launch picker.
pub async fn list_profiles(data_dir: &Path) -> Result<ProfileListResponse> {
    let params = ProfileListRequest {};
    params.validate()?;
    let reply: ProfileListResponse =
        eliot_daemon::client_call_typed(data_dir, "profile.list", &params).await?;
    params.validate_response(&reply)?;
    Ok(reply)
}

/// Rules of one namespace (global when `project` is absent), paging until
/// an empty page, at most `RULE_PAGES` pages; `true` when more remain.
pub async fn list_rules(
    data_dir: &Path,
    project: Option<String>,
) -> Result<(Vec<RuleListEntry>, bool)> {
    const RULE_PAGES: usize = 5;
    let mut rules = Vec::new();
    let mut after = None;
    for _ in 0..RULE_PAGES {
        let params = RuleListRequest {
            project: project.clone(),
            after: after.clone(),
            limit: 200,
        };
        params.validate()?;
        let reply: RuleListResponse =
            eliot_daemon::client_call_typed(data_dir, "rule.list", &params).await?;
        params.validate_response(&reply)?;
        if reply.rules.is_empty() {
            return Ok((rules, false));
        }
        after = reply.next.clone();
        rules.extend(reply.rules);
        if after.is_none() {
            return Ok((rules, false));
        }
    }
    Ok((rules, true))
}

/// Existing script entries under the approved Run roots.
pub async fn list_scripts(data_dir: &Path) -> Result<ScriptListResponse> {
    let mut params = ScriptListRequest::default();
    let mut roots: Vec<eliot_core::api::ScriptRootView> = Vec::new();
    loop {
        params.validate()?;
        let reply: ScriptListResponse =
            eliot_daemon::client_call_typed(data_dir, "script.list", &params).await?;
        params.validate_response(&reply)?;
        for root in reply.roots {
            if let Some(existing) = roots.iter_mut().find(|r| r.project == root.project) {
                existing.entries.extend(root.entries);
                existing.truncated |= root.truncated;
            } else {
                roots.push(root);
            }
        }
        match reply.next_cursor {
            Some(next) => params.cursor = Some(next),
            None => break,
        }
    }
    Ok(ScriptListResponse {
        roots,
        next_cursor: None,
    })
}

/// One script entry's text and revision.
pub async fn read_script(
    data_dir: &Path,
    project: Option<String>,
    path: String,
) -> Result<ScriptReadResponse> {
    let params = ScriptReadRequest { project, path };
    params.validate()?;
    let reply: ScriptReadResponse =
        eliot_daemon::client_call_typed(data_dir, "script.read", &params).await?;
    params.validate_response(&reply)?;
    Ok(reply)
}

/// Validate (dry run) or save one script entry from `expected_revision`.
pub async fn save_script(
    data_dir: &Path,
    project: Option<String>,
    path: String,
    expected_revision: String,
    content: String,
    dry_run: bool,
) -> Result<ScriptSaveResponse> {
    let params = ScriptSaveRequest {
        project,
        path,
        expected_revision,
        content,
        dry_run,
    };
    params.validate()?;
    let reply: ScriptSaveResponse =
        eliot_daemon::client_call_typed(data_dir, "script.save", &params).await?;
    params.validate_response(&reply)?;
    Ok(reply)
}

/// Run one explicit occurrence of a timer (the existing Owner Run path).
pub async fn run_timer(
    data_dir: &Path,
    mutation_id: String,
    project: Option<String>,
    rule: &str,
) -> Result<Command> {
    let request = TimerRunRequest::new(mutation_id, rule);
    let request = match project {
        Some(project) => request.in_project(project),
        None => request,
    };
    request.validate()?;
    let reply: Command = eliot_daemon::client_call_typed(data_dir, "timer.run", &request).await?;
    request.validate_response(&reply)?;
    Ok(reply)
}
