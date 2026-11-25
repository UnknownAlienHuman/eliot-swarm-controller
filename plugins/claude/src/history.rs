//! Exact historical interrupt evidence from Claude's main transcript.

use eliot_core::{Error, Result, api::TurnResolveStatus};
use eliot_plugin::{
    protocol::NativeTurnEvidenceRequest,
    turn_readback::{find_exact_history, is_native_uuid, read_history_records},
};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    path::PathBuf,
    time::{Duration, Instant},
};

pub(crate) async fn read(raw: &Value) -> Result<Value> {
    let request: NativeTurnEvidenceRequest = serde_json::from_value(raw.clone())
        .map_err(|_| Error::new("invalid", "Invalid Claude history binding"))?;
    tokio::task::spawn_blocking(move || {
        let outcome = interrupted(&request)?;
        Ok(
            json!({"native_scope":request.native_scope,"native_ref":request.native_ref,
            "turn_id":request.turn_id,"outcome":outcome.then_some(TurnResolveStatus::Interrupted)}),
        )
    })
    .await
    .map_err(|_| Error::new("unavailable", "Claude history reader stopped"))?
}

#[derive(Default)]
struct Node {
    parent: Option<String>,
    prompt: Option<String>,
    input: bool,
    sdk_input: bool,
    interrupt: bool,
}

fn interrupted(r: &NativeTurnEvidenceRequest) -> Result<bool> {
    if r.profile["harness"] != "claude"
        || !is_native_uuid(&r.native_ref)
        || !is_native_uuid(&r.turn_id)
    {
        return Ok(false);
    }
    let get_env = |key: &str| r.target_environment[key].as_str().filter(|s| !s.is_empty());
    let config = get_env("CLAUDE_CONFIG_DIR");
    if crate::host::claude_scope(config) != r.native_scope {
        return Ok(false);
    }
    let roots = if let Some(config) = config {
        vec![PathBuf::from(config)]
    } else {
        ["HOME", "USERPROFILE"]
            .into_iter()
            .filter_map(get_env)
            .map(|home| PathBuf::from(home).join(".claude"))
            .collect()
    };
    let mut roots: Vec<_> = roots
        .into_iter()
        .filter(|r| r.is_absolute())
        .filter_map(|r| r.canonicalize().ok())
        .collect();
    roots.sort();
    roots.dedup();
    let deadline = Instant::now() + Duration::from_secs(5);
    let filename = format!("{}.jsonl", r.native_ref);
    let Some(path) = find_exact_history(
        roots.iter().map(|r| r.join("projects")),
        1,
        |name| name == filename,
        deadline,
    )?
    else {
        return Ok(false);
    };
    let Some(root) = roots
        .iter()
        .find(|root| path.starts_with(root.join("projects")))
    else {
        return Ok(false);
    };
    let mut nodes = HashMap::new();
    read_history_records(&path, &root.join("projects"), deadline, |record| {
        if record["sessionId"].as_str() != Some(r.native_ref.as_str())
            || record["isSidechain"].as_bool() != Some(false)
        {
            return Ok(());
        }
        let Some(id) = record["uuid"].as_str().filter(|s| is_native_uuid(s)) else {
            return Ok(());
        };
        let user = record["type"] == "user" && record["message"]["role"] == "user";
        let content = record["message"]["content"].as_array();
        let tool_result = content.is_some_and(|parts| {
            !parts.is_empty() && parts.iter().all(|part| part["type"] == "tool_result")
        });
        let marker=user && record.get("turnOrigin").is_none() && record.get("turnPromptId").is_none()
            && content.is_some_and(|parts| matches!(parts.as_slice(),[part]
                if part["type"]=="text" && matches!(part["text"].as_str(),
                    Some("[Request interrupted by user]" | "[Request interrupted by user for tool use]"))));
        let node = Node {
            parent: record["parentUuid"]
                .as_str()
                .filter(|id| is_native_uuid(id))
                .map(str::to_owned),
            prompt: record["promptId"]
                .as_str()
                .filter(|id| is_native_uuid(id))
                .map(str::to_owned),
            input: user && !tool_result && !marker,
            sdk_input: record["turnOrigin"] == "sdk"
                && record["turnPromptId"].as_str().is_some_and(|id| {
                    is_native_uuid(id) && record["promptId"].as_str() == Some(id)
                }),
            interrupt: marker,
        };
        if nodes.insert(id.to_owned(), node).is_some() {
            return Err(Error::new(
                "source_integrity",
                "Claude transcript repeats a native message identity",
            ));
        }
        Ok(())
    })?;
    let Some(input) = nodes
        .get(&r.turn_id)
        .filter(|node| node.input && node.sdk_input && node.prompt.is_some())
    else {
        return Ok(false);
    };
    Ok(nodes
        .values()
        .filter(|node| node.interrupt && node.prompt == input.prompt)
        .any(|marker| {
            let mut parent = marker.parent.as_deref();
            for _ in 0..nodes.len() {
                let Some(id) = parent else {
                    break;
                };
                if id == r.turn_id {
                    return true;
                }
                let Some(node) = nodes.get(id).filter(|node| !node.input) else {
                    break;
                };
                parent = node.parent.as_deref();
            }
            false
        }))
}
