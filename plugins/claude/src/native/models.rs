//! No-prompt metadata read over the Claude Code initialize control request.

use std::{ffi::OsString, path::Path, time::Duration};

use eliot_core::{Error, Result, SecretPolicy};
use eliot_plugin::{
    native_transport::NativeLaunch,
    protocol::{ModelListParams, ModelListPart},
};
use serde_json::{Value, json};
use tokio::time::Instant;

use super::{ClaudeProcess, types::InitializeReadback};

pub(crate) async fn read(raw: &Value, workspace: &Path) -> Result<Value> {
    let params: ModelListParams = serde_json::from_value(raw.clone())
        .map_err(|_| Error::new("invalid", "Claude model.list params are invalid"))?;
    if params.part != ModelListPart::Models || params.profile["harness"].as_str() != Some("claude")
    {
        return Err(Error::new(
            "unsupported",
            "Claude model.list requires a Claude models request",
        ));
    }
    let cmd = params.profile["cmd"]
        .as_str()
        .filter(|cmd| Path::new(cmd).is_absolute())
        .ok_or_else(|| {
            Error::new(
                "profile_error",
                "Claude model.list requires a frozen native cmd",
            )
        })?;
    // The host's ModelListParams does not carry the resolved private
    // environment. It would be wrong to substitute the default account for
    // any profile that can select a different route through pass_env.
    if params
        .profile
        .get("pass_env")
        .is_some_and(|value| value.as_array().is_none_or(|values| !values.is_empty()))
    {
        return Err(Error::new(
            "unavailable",
            "Claude model.list needs the profile's resolved private environment",
        ));
    }

    let args = [
        "-p",
        "--input-format",
        "stream-json",
        "--output-format",
        "stream-json",
        "--verbose",
        "--no-session-persistence",
        "--safe-mode",
        "--tools",
        "",
        "--strict-mcp-config",
        "--mcp-config",
        r#"{"mcpServers":{}}"#,
    ];
    let mut native = ClaudeProcess::spawn(
        NativeLaunch {
            program: OsString::from(cmd),
            args: args.into_iter().map(OsString::from).collect(),
            cwd: workspace.to_owned(),
            environment: vec![("ENABLE_CLAUDEAI_MCP_SERVERS".into(), "false".into())],
        },
        SecretPolicy::default(),
    )
    .await
    .map_err(|_| {
        Error::new(
            "unavailable",
            "Claude model.list could not start the native reader",
        )
    })?;

    let reply = native
        .client()
        .control(
            "initialize",
            json!({}),
            Instant::now() + Duration::from_secs(12),
        )
        .await;
    native
        .terminate(true, Instant::now() + Duration::from_secs(3))
        .await?;
    let reply = reply.map_err(|_| {
        Error::new(
            "unavailable",
            "Claude model.list has no native initialize reply",
        )
    })?;
    project_inventory(reply)
}

fn project_inventory(reply: Value) -> Result<Value> {
    let readback: InitializeReadback = serde_json::from_value(reply).map_err(|_| {
        Error::new(
            "unavailable",
            "Claude model.list received an invalid inventory",
        )
    })?;
    if readback.models.is_empty() || readback.models.len() > 1024 {
        return Err(Error::new(
            "unavailable",
            "Claude model.list received no bounded native inventory",
        ));
    }
    let models: Vec<_> = readback
        .models
        .into_iter()
        .filter_map(|model| {
            let id = model.value.filter(|id| !id.trim().is_empty())?;
            Some(json!({
                "id": id,
                "resolved_model": model.resolved_model,
                "display_name": model.display_name,
                "efforts": model.supported_effort_levels,
            }))
        })
        .collect();
    if models.is_empty() {
        return Err(Error::new(
            "unavailable",
            "Claude model.list received no usable native models",
        ));
    }
    // Initialize reports choices and capabilities, not whether inference is
    // currently entitled, and list order is not default-model evidence.
    Ok(json!({"models": models, "default": null}))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inventory_projects_native_ids_and_only_reported_effort_choices() {
        let inventory = project_inventory(json!({
            "models": [
                {"value":"alias", "resolvedModel":"provider-model",
                 "displayName":"Native Name", "supportedEffortLevels":["low", "high"]},
                {"value":"gateway/custom"}
            ],
            "account": {"irrelevant": "identity"}
        }))
        .expect("native inventory");
        assert_eq!(
            inventory,
            json!({"default":null,"models":[
                {"id":"alias","resolved_model":"provider-model",
                 "display_name":"Native Name","efforts":["low","high"]},
                {"id":"gateway/custom","resolved_model":null,"display_name":null,"efforts":null}
            ]})
        );
        assert!(project_inventory(json!({"session_state":"idle"})).is_err());
    }
}
