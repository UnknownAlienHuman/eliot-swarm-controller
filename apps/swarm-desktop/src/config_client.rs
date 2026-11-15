//! The one client module behind the Settings page.
//!
//! Every read and write the settings UI makes goes through here: the
//! desktop preferences file ([`crate::settings`]) and the daemon's existing
//! configuration APIs (`profile.list`, `config.profiles.read/save`,
//! `project.list`, `rule.list`, `script.list/read/save`, `timer.run`), each
//! with the same request/response validators the CLI runs. When the unified
//! `settings.read`/`settings.save` API (k55) lands it replaces these calls
//! in this module only; the UI keeps its shape.

use std::path::{Path, PathBuf};

use eliot_core::api::{
    ProfileListRequest, ProfileListResponse, ProfilesReadRequest, ProfilesReadResponse,
    ProfilesSaveRequest, ProfilesSaveResponse, ProjectRegistrationView, RuleListEntry,
    RuleListRequest, RuleListResponse, ScriptListRequest, ScriptListResponse, ScriptReadRequest,
    ScriptReadResponse, ScriptSaveRequest, ScriptSaveResponse, TimerRunRequest,
    discovery::ProjectListRequest,
};
use eliot_core::{Command, Error, Result};
use serde_json::Value;

use crate::settings::{self, Settings};

/// The desktop preferences (defaults when there is no usable file).
pub fn load_preferences() -> Settings {
    settings::load()
}

/// Write the desktop preferences.
pub fn save_preferences(preferences: &Settings) -> std::result::Result<(), String> {
    settings::save(preferences)
}

/// Where the desktop preferences live, for the Advanced section.
pub fn preferences_path() -> Option<PathBuf> {
    settings::path()
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

/// Sanitized configured-profile summaries for the launch picker.
pub async fn list_profiles(data_dir: &Path) -> Result<ProfileListResponse> {
    let params = ProfileListRequest {};
    params.validate()?;
    let reply: ProfileListResponse =
        eliot_daemon::client_call_typed(data_dir, "profile.list", &params).await?;
    params.validate_response(&reply)?;
    Ok(reply)
}

/// The `[profiles.*]` source text and the file revision.
pub async fn read_profiles_source(data_dir: &Path) -> Result<ProfilesReadResponse> {
    let params = ProfilesReadRequest {};
    params.validate()?;
    let reply: ProfilesReadResponse =
        eliot_daemon::client_call_typed(data_dir, "config.profiles.read", &params).await?;
    params.validate_response(&reply)?;
    Ok(reply)
}

/// Validate (dry run) or save the profile draft from `expected_revision`.
pub async fn save_profiles(
    data_dir: &Path,
    expected_revision: Option<String>,
    source: String,
    dry_run: bool,
) -> Result<ProfilesSaveResponse> {
    let params = ProfilesSaveRequest {
        expected_revision,
        source,
        dry_run,
    };
    params.validate()?;
    let reply: ProfilesSaveResponse =
        eliot_daemon::client_call_typed(data_dir, "config.profiles.save", &params).await?;
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
