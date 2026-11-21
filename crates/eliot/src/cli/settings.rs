//! Unified settings use the daemon's canonical request and response validators.
use std::{collections::BTreeMap, io::Read, path::Path, path::PathBuf};

use eliot_core::api::{
    REDACTION_MARKER, SettingsClientSource, SettingsReadRequest, SettingsReadResponse,
    SettingsSaveRequest, SettingsSaveResponse, SettingsSection,
};

use super::args::{ClaudeCmdArgs, SettingsArgs, SettingsCommand};
use super::support::{Failure, print_value};
use serde_json::Value;

pub(crate) async fn run(
    args: SettingsArgs,
    data_dir: PathBuf,
    json_output: bool,
) -> Result<u8, Failure> {
    let value = match args.command {
        SettingsCommand::Read => {
            let request = SettingsReadRequest::default();
            request.validate().map_err(Failure::for_read)?;
            let reply: SettingsReadResponse =
                eliot_daemon::client_call_typed(&data_dir, "settings.read", &request)
                    .await
                    .map_err(Failure::for_read)?;
            request
                .validate_response(&reply)
                .map_err(Failure::invalid_response)?;
            serde_json::to_value(reply).map_err(|_| Failure::new(4, "invalid settings response"))?
        }
        SettingsCommand::Save { file, dry_run } => {
            let mut bytes = Vec::new();
            std::fs::File::open(&file)
                .and_then(|f| f.take(128 * 1024 + 1).read_to_end(&mut bytes))
                .map_err(|e| Failure::new(2, format!("read settings request: {e}")))?;
            if bytes.len() > 128 * 1024 {
                return Err(Failure::new(2, "settings request exceeds 128 KiB"));
            }
            let mut request: SettingsSaveRequest = serde_json::from_slice(&bytes)
                .map_err(|_| Failure::new(2, "invalid settings.save JSON request"))?;
            request.client = SettingsClientSource::Cli;
            request.dry_run |= dry_run;
            request.validate().map_err(Failure::for_mutation)?;
            let reply: SettingsSaveResponse =
                eliot_daemon::client_call_typed(&data_dir, "settings.save", &request)
                    .await
                    .map_err(Failure::for_mutation)?;
            request
                .validate_response(&reply)
                .map_err(Failure::invalid_response)?;
            serde_json::to_value(reply).map_err(|_| Failure::new(4, "invalid settings response"))?
        }
        SettingsCommand::ClaudeCmd(args) => update_claude_cmd(&data_dir, args).await?,
    };
    print_value(&value, json_output);
    Ok(0)
}

async fn update_claude_cmd(data_dir: &Path, args: ClaudeCmdArgs) -> Result<Value, Failure> {
    let read_request = SettingsReadRequest {
        sections: Some(vec![SettingsSection::Profiles]),
    };
    read_request.validate().map_err(Failure::for_read)?;
    let read: SettingsReadResponse =
        eliot_daemon::client_call_typed(data_dir, "settings.read", &read_request)
            .await
            .map_err(Failure::for_read)?;
    read_request
        .validate_response(&read)
        .map_err(Failure::invalid_response)?;
    if let Some(issue) = read
        .issues
        .iter()
        .find(|issue| issue.section == Some(SettingsSection::Profiles))
    {
        return Err(Failure::new(
            2,
            format!("profiles settings are not editable: {}", issue.message),
        ));
    }
    let mut profiles = read
        .sections
        .get(&SettingsSection::Profiles)
        .cloned()
        .ok_or_else(|| Failure::new(2, "profiles settings are unavailable"))?;
    if profiles.to_string().contains(REDACTION_MARKER) {
        return Err(Failure::new(
            2,
            "profiles settings contain redacted values; use the full settings editor",
        ));
    }
    let profile = profiles
        .as_object_mut()
        .and_then(|profiles| profiles.get_mut(&args.profile))
        .and_then(Value::as_object_mut)
        .ok_or_else(|| Failure::new(2, format!("Claude profile {} was not found", args.profile)))?;
    if profile.get("harness").and_then(Value::as_str) != Some("claude") {
        return Err(Failure::new(
            2,
            format!("profile {} is not a Claude profile", args.profile),
        ));
    }
    let cmd = if args.latest {
        "latest"
    } else {
        args.cmd
            .as_deref()
            .ok_or_else(|| Failure::new(2, "provide --cmd PATH or --latest"))?
    };
    profile.insert("cmd".to_owned(), Value::String(cmd.to_owned()));

    let mut sections = BTreeMap::new();
    sections.insert(SettingsSection::Profiles, profiles);
    let save_request = SettingsSaveRequest {
        expected_revision: read.revision,
        sections,
        dry_run: false,
        client: SettingsClientSource::Cli,
    };
    save_request.validate().map_err(Failure::for_mutation)?;
    let saved: SettingsSaveResponse =
        eliot_daemon::client_call_typed(data_dir, "settings.save", &save_request)
            .await
            .map_err(Failure::for_mutation)?;
    save_request
        .validate_response(&saved)
        .map_err(Failure::invalid_response)?;
    serde_json::to_value(saved).map_err(|_| Failure::new(4, "invalid settings response"))
}
