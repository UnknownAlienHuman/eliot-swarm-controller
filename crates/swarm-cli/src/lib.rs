//! Thin standalone client entry points shared by the CLI binary.
//!
//! This package forwards requests over authenticated local IPC. It does not
//! open the Store, apply role policy, or start the host. Only `hook.emit` may
//! replay, using its Store-deduplicated stable event identity.

use serde_json::{Value, json};
use std::{path::Path, time::Duration};
use swarm_client::{Client, IpcConfig};
use swarm_contracts::{
    Credential,
    error::{Error, Result},
};

/// Send an application request through the existing authenticated IPC client.
/// Ordinary calls are sent once; only `hook.emit` uses the Store's durable
/// `(source_id, commit_oid)` identity for the existing bounded replay path.
pub async fn call(
    data_dir: &Path,
    credential: &Credential,
    ipc: &IpcConfig,
    method: &str,
    params: Value,
) -> Result<Value> {
    if method == "hook.emit" {
        return hook_emit_with_retry(data_dir, credential, ipc, params).await;
    }
    let mut client = Client::connect(data_dir, credential, ipc).await?;
    client.request(method, params).await
}

/// Reject a generic call whose root CLI path has private setup semantics.
pub fn validate_call_method(method: &str) -> Result<()> {
    if method == "hook.source.setup" {
        return Err(Error::invalid(
            "use `swarm hook setup` so the one-time source credential is written privately and redacted from output",
        ));
    }
    Ok(())
}

/// Prepare generic `swarm call` parameters using the current CLI request-ID
/// rules. This is request correlation only; authorization and validation remain
/// on the host. The returned ID is printed by the binary before the IPC call.
pub fn prepare_call(
    method: &str,
    mut params: Value,
    request_id: Option<&str>,
) -> Result<(Value, Option<String>)> {
    validate_call_method(method)?;
    if method == "hook.emit" {
        if request_id.is_some() {
            return Err(Error::invalid("--request-id is not accepted for hook emit"));
        }
        return Ok((hook_emit_params(&params)?, None));
    }

    let read_only =
        swarm_mcp::application_method_read_only(method).unwrap_or(method == "doctor.inspect");
    if read_only {
        return Ok((params, None));
    }
    if !params.is_object() {
        return Err(Error::invalid("params file must contain an object"));
    }

    if method == "swarm.launch" {
        let original_id = required_text(&params, "client_request_id")?;
        required_text(&params, "plan_digest")?;
        if request_id.is_some_and(|requested| requested != original_id) {
            return Err(Error::invalid(
                "--request-id must match the client_request_id in the swarm.launch params file",
            ));
        }
        return Ok((params, Some(original_id.to_owned())));
    }

    if let Some(request_id) = request_id {
        params["client_request_id"] = Value::String(request_id.to_owned());
    } else if params.get("client_request_id").is_none() {
        params["client_request_id"] = Value::String(uuid::Uuid::new_v4().to_string());
    }
    let prepared_id = required_text(&params, "client_request_id")?.to_owned();
    Ok((params, Some(prepared_id)))
}

const HOOK_EMIT_RETRY_DELAY_MS: [Option<u64>; 3] = [Some(100), Some(400), None];

/// The Store deduplicates an exact source/commit event and checks the
/// canonical retained payload before acknowledging a duplicate. Keep retries
/// scoped to this method and never change the source event identity.
async fn hook_emit_with_retry(
    data_dir: &Path,
    credential: &Credential,
    ipc: &IpcConfig,
    params: Value,
) -> Result<Value> {
    let exact_params = hook_emit_params(&params)?;
    let source_id = required_text(&exact_params, "source_id")?.to_owned();
    let commit_oid = required_text(&exact_params, "commit_oid")?.to_owned();

    for retry_delay_ms in HOOK_EMIT_RETRY_DELAY_MS {
        match swarm_client::call(data_dir, credential, "hook.emit", exact_params.clone(), ipc).await
        {
            Ok(reply) if hook_emit_ack_matches(&reply, &source_id, &commit_oid) => {
                return Ok(reply);
            }
            Ok(_) => {
                return Err(Error::new(
                    "HOOK_EMIT_ACK_INVALID",
                    "hook acknowledgment did not identify the requested source and commit",
                ));
            }
            Err(error) if matches!(error.code.as_str(), "HOST_UNAVAILABLE" | "OUTCOME_UNKNOWN") => {
                let Some(delay_ms) = retry_delay_ms else {
                    return Err(Error::new(
                        error.code,
                        "post-commit fact remains unconfirmed after bounded same-identity retry; a later replay of this source and commit is deduplicated",
                    ));
                };
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
            Err(error) => return Err(error),
        }
    }

    Err(Error::new(
        "HOOK_EMIT_RETRY_EXHAUSTED",
        "post-commit fact did not receive a durable acknowledgment",
    ))
}

/// Match the root CLI's closed request shape and strip its unused optional
/// client request ID. Store-owned source/commit identity is the only retry key.
fn hook_emit_params(params: &Value) -> Result<Value> {
    let object = params
        .as_object()
        .ok_or_else(|| Error::invalid("params must be an object"))?;
    for key in object.keys() {
        if !["source_id", "commit_oid", "client_request_id"].contains(&key.as_str()) {
            return Err(Error::invalid(format!("unknown field: {key}")));
        }
    }
    let source_id = required_text(params, "source_id")?;
    let commit_oid = required_text(params, "commit_oid")?;
    Ok(json!({"source_id":source_id,"commit_oid":commit_oid}))
}

fn hook_emit_ack_matches(reply: &Value, source_id: &str, commit_oid: &str) -> bool {
    let (Some(recorded), Some(duplicate)) = (
        reply.get("recorded").and_then(Value::as_bool),
        reply.get("duplicate").and_then(Value::as_bool),
    ) else {
        return false;
    };
    reply["event"] == "git.post_commit"
        && reply["source_id"].as_str() == Some(source_id)
        && reply["commit_oid"]
            .as_str()
            .is_some_and(|oid| oid.eq_ignore_ascii_case(commit_oid))
        && reply["readback_verified"] == true
        && reply["observation_id"].as_i64().is_some_and(|id| id > 0)
        && recorded != duplicate
}

fn required_text<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty())
        .ok_or_else(|| Error::invalid(format!("{field} must be a nonempty string")))
}
