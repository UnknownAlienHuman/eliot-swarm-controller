//! Read exact terminal records without starting or updating a Codex server.

use eliot_core::{Error, NativeStoreBinding, Result, api::TurnResolveStatus};
use eliot_plugin::{
    protocol::NativeTurnEvidenceRequest,
    turn_readback::{find_exact_history, is_native_uuid, read_history_records},
};
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    time::{Duration, Instant},
};

pub(crate) async fn read(raw: &Value) -> Result<Value> {
    let request: NativeTurnEvidenceRequest = serde_json::from_value(raw.clone())
        .map_err(|_| Error::new("invalid", "Invalid Codex history binding"))?;
    tokio::task::spawn_blocking(move || {
        let outcome = terminal(&request)?;
        Ok(
            json!({"native_scope":request.native_scope,"native_ref":request.native_ref,
            "turn_id":request.turn_id,"outcome":outcome}),
        )
    })
    .await
    .map_err(|_| Error::new("unavailable", "Codex history reader stopped"))?
}

fn selected_home(r: &NativeTurnEvidenceRequest) -> Result<Option<PathBuf>> {
    let get_env = |key: &str| r.target_environment[key].as_str().filter(|s| !s.is_empty());
    let binding = r
        .profile
        .get("native_store_binding")
        .map(|value| serde_json::from_value::<NativeStoreBinding>(value.clone()))
        .transpose()
        .map_err(|_| {
            Error::new(
                "native_scope_unverified",
                "Invalid retained Codex store binding",
            )
        })?;
    let home_paths = if let Some(binding) = &binding {
        if binding.native_scope != r.native_scope {
            return Ok(None);
        }
        vec![PathBuf::from(&binding.home)]
    } else if let Some(home) = get_env("CODEX_HOME") {
        vec![PathBuf::from(home)]
    } else {
        ["USERPROFILE", "HOME"]
            .into_iter()
            .filter_map(get_env)
            .map(|home| PathBuf::from(home).join(".codex"))
            .collect()
    };
    let mut selected = Vec::new();
    for path in home_paths {
        if !path.is_absolute() {
            continue;
        }
        let Ok(home) = path.canonicalize() else {
            continue;
        };
        let root = binding
            .as_ref()
            .map(|b| b.root.as_str())
            .or_else(|| get_env("CODEX_SQLITE_HOME"));
        let root = match root {
            Some(path) if PathBuf::from(path).is_absolute() => {
                match PathBuf::from(path).canonicalize() {
                    Ok(root) => root,
                    Err(_) => continue,
                }
            }
            Some(_) => continue,
            None => home.clone(),
        };
        let Some(home_text) = home.to_str() else {
            continue;
        };
        if crate::discovery::store_scope(home_text, &home, &root)? == r.native_scope
            && !selected.contains(&home)
        {
            selected.push(home);
        }
    }
    Ok(if selected.len() == 1 {
        selected.pop()
    } else {
        None
    })
}

fn terminal(r: &NativeTurnEvidenceRequest) -> Result<Option<TurnResolveStatus>> {
    use TurnResolveStatus::{Completed, Failed, Interrupted};
    if r.profile["harness"] != "codex"
        || !is_native_uuid(&r.native_ref)
        || !is_native_uuid(&r.turn_id)
    {
        return Ok(None);
    }
    let Some(home) = selected_home(r)? else {
        return Ok(None);
    };
    let deadline = Instant::now() + Duration::from_secs(5);
    let suffix = format!("-{}.jsonl", r.native_ref);
    let Some(path) = find_exact_history(
        [home.join("sessions"), home.join("archived_sessions")],
        4,
        |name| name.starts_with("rollout-") && name.ends_with(&suffix),
        deadline,
    )?
    else {
        return Ok(None);
    };
    let Some(root) = path
        .strip_prefix(&home)
        .ok()
        .and_then(|p| p.components().next())
        .map(|c| home.join(c.as_os_str()))
    else {
        return Ok(None);
    };
    let mut header = false;
    let mut result = None;
    read_history_records(&path, &root, deadline, |record| {
        let kind = record["type"].as_str();
        let p = &record["payload"];
        if !header {
            if kind != Some("session_meta") || p["id"].as_str() != Some(r.native_ref.as_str()) {
                return Err(Error::new(
                    "source_integrity",
                    "Codex rollout header does not bind the requested session",
                ));
            }
            header = true;
            return Ok(());
        }
        if kind != Some("event_msg") || p["turn_id"].as_str() != Some(r.turn_id.as_str()) {
            return Ok(());
        }
        let status = match p["type"].as_str() {
            Some("task_complete" | "turn_complete") => match p.get("error") {
                None | Some(Value::Null) => Completed,
                Some(Value::Object(e)) if e.get("message").and_then(Value::as_str).is_some() => {
                    Failed
                }
                _ => {
                    return Err(Error::new(
                        "source_integrity",
                        "Invalid exact Codex terminal",
                    ));
                }
            },
            Some("task_aborted" | "turn_aborted") => match p["reason"].as_str() {
                Some(reason) if crate::native::types::known_turn_abort_reason(reason) => {
                    Interrupted
                }
                _ => {
                    return Err(Error::new(
                        "source_integrity",
                        "Unrecognized exact Codex abort",
                    ));
                }
            },
            _ => return Ok(()),
        };
        if result
            .replace(status)
            .is_some_and(|before| before != status)
        {
            return Err(Error::new(
                "source_integrity",
                "Conflicting Codex terminals for one turn",
            ));
        }
        Ok(())
    })?;
    Ok(result)
}
