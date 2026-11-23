//! Provider model evidence from the owned MSP session's raw live stream.

use eliot_core::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::time::Instant;

use crate::native::NativeClient;

#[derive(Serialize, Deserialize)]
struct StreamRef<'a> {
    kind: &'a str,
    id: &'a str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SubscribeParams<'a> {
    session_id: &'a str,
    stream: StreamRef<'a>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Subscription {
    subscription_id: u64,
}

pub(super) async fn subscribe(
    client: &NativeClient,
    session_id: &str,
    deadline: Instant,
) -> Result<u64> {
    let params = SubscribeParams {
        session_id,
        stream: StreamRef {
            kind: "session.live",
            id: session_id,
        },
    };
    let params = serde_json::to_value(params)
        .map_err(|_| Error::new("internal", "Muse log/subscribe params could not be encoded"))?;
    let reply = client.call("log/subscribe", params, deadline).await?;
    let result: Subscription = serde_json::from_value(reply).map_err(|_| {
        Error::new(
            "native_protocol",
            "Muse log/subscribe omitted its subscription identity",
        )
    })?;
    Ok(result.subscription_id)
}

pub(super) enum Fact<'a> {
    ForegroundTask(&'a str),
    TaskEnded(&'a str),
    Response { task: &'a str, model: &'a str },
}

pub(super) struct Signal<'a> {
    pub subscription_id: u64,
    pub session_id: &'a str,
    pub turn_id: &'a str,
    pub fact: Fact<'a>,
}

pub(super) fn signal(params: &Value) -> Option<Signal<'_>> {
    let subscription_id = params.get("subscriptionId")?.as_u64()?;
    let record = params.get("record")?;
    if record.get("payload_type")?.as_str()? != "runtime.session"
        || record.get("payload_schema_version")?.as_u64()? != 1
    {
        return None;
    }
    // Even on the live subscription, the envelope retains the physical
    // session stream identity.
    let stream: StreamRef<'_> = StreamRef::deserialize(record.get("stream")?).ok()?;
    if stream.kind != "session" {
        return None;
    }
    let payload = record.get("payload")?;
    let turn_id = payload.get("run_id")?.as_str()?;
    let event = payload.get("event")?;
    let fact = match (payload.get("kind")?.as_str()?, event.get("kind")?.as_str()?) {
        ("run", "task_stream_linked") if event.get("execution_mode")?.as_str()? == "foreground" => {
            Fact::ForegroundTask(event.get("task_id")?.as_str()?)
        }
        ("task", "completed" | "failed" | "cancelled") => {
            Fact::TaskEnded(payload.get("task_id")?.as_str()?)
        }
        ("task", "status") => {
            let task = payload.get("task_id")?.as_str()?;
            if event.get("task_id")?.as_str()? != task {
                return None;
            }
            let details = event.get("details")?;
            // Retry/failure records can echo the request's model when no
            // provider response was received. Accept a successful provider
            // stream only, with a response identity.
            if details.get("phase")?.as_str()? != "stream_succeeded" {
                return None;
            }
            let model = details
                .get("facets")?
                .as_array()?
                .iter()
                .find_map(|facet| {
                    let detail = facet.get("detail")?;
                    if detail.get("kind")?.as_str()? != "provider"
                        || detail.get("response_id")?.as_str()?.trim().is_empty()
                    {
                        return None;
                    }
                    detail
                        .get("model")?
                        .as_str()
                        .filter(|id| !id.trim().is_empty())
                })?;
            Fact::Response { task, model }
        }
        _ => return None,
    };
    Some(Signal {
        subscription_id,
        session_id: stream.id,
        turn_id,
        fact,
    })
}
