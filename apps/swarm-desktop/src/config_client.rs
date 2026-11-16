//! The one client module behind the Settings page.
//!
//! Every setting the desktop reads or writes goes through the daemon's
//! unified `settings.read` / `settings.save` (one store, revision-guarded,
//! with author history), including the desktop's own `[desktop]`
//! preferences ([`crate::settings`]). The remaining calls are the runtime
//! reads and file-owned APIs the settings document does not cover:
//! `profile.list` (launchability), `project.list` (resolved registrations),
//! `routing.preview`, `rule.list`, `script.list/read/save` and `timer.run`.
//! Each runs the same request/response validators the CLI runs.

use std::path::Path;

use eliot_core::api::harness::MAX_CATALOG_LIMIT;
use eliot_core::api::{
    HarnessQuotaRequest, HarnessQuotaResponse, ModelsCatalogRequest, ModelsCatalogResponse,
    ProfileListRequest, ProfileListResponse, ProjectRegistrationView, RoutingPreviewRequest,
    RoutingPreviewResponse, RuleListEntry, RuleListRequest, RuleListResponse, ScriptListRequest,
    ScriptListResponse, ScriptReadRequest, ScriptReadResponse, ScriptSaveRequest,
    ScriptSaveResponse, SettingsReadResponse, SettingsSaveRequest, SettingsSaveResponse,
    TimerRunRequest, discovery::ProjectListRequest,
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

/// Pages of `models.catalog` the picker reads at most (100 models each:
/// the daemon's 1024-model aggregate bound).
const CATALOG_PAGES: usize = 11;

/// The unified model catalogue for the composer's picker: every installed
/// harness with its version, and every model page by page (the daemon
/// caches and coalesces the native reads).
pub async fn models_catalog(data_dir: &Path) -> Result<ModelsCatalogResponse> {
    let mut request = ModelsCatalogRequest {
        query: None,
        after: None,
        limit: Some(MAX_CATALOG_LIMIT),
    };
    let mut whole: Option<ModelsCatalogResponse> = None;
    for _ in 0..CATALOG_PAGES {
        request.validate()?;
        let page: ModelsCatalogResponse =
            eliot_daemon::client_call_typed(data_dir, "models.catalog", &request).await?;
        request.validate_response(&page)?;
        let next = page.next.clone();
        match whole.as_mut() {
            Some(whole) => whole.models.extend(page.models),
            None => whole = Some(page),
        }
        match next {
            Some(after) => request.after = Some(after),
            None => break,
        }
    }
    let mut whole = whole.ok_or_else(|| Error::new("unknown", "no catalogue page was read"))?;
    whole.next = None;
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
