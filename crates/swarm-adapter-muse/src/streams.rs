//! Small notification projection, not a copied Session/transcript facade.
//!
//! Primary wire source: Muse SDK 1.3.0, a7c10c5dd3f66be412077d29f9d11111af70317b,
//! schema/msp/stable/msp.schema.json (ItemDeltaParams, Item,
//! SessionTokenUsageParams), read 2026-10-16. Deltas have no turnId or revision;
//! their Operation mapping must come from the caller's known Root Task item.
use crate::msp::Notification;
use serde_json::Value;
use swarm_client::ModuleLink;
use swarm_contracts::{
    error::{Error, Result},
    native_stream::*,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KnownItemKind { AgentMessage, Reasoning, ToolCall, HiddenReasoning }

/// Facts already joined to an exact Root Task Operation by the adapter owner.
/// Merely receiving an item/session id never creates this relationship.
pub struct KnownRootTaskItem {
    pub item_id: String,
    pub turn_id: String,
    pub item_order: u32,
    pub kind: KnownItemKind,
}

pub struct KnownRootTaskStream {
    pub scope: StreamScope,
    pub item: Option<KnownRootTaskItem>,
}

fn text<'a>(value: &'a Value, key: &str) -> Result<&'a str> {
    value[key].as_str().filter(|s| !s.is_empty())
        .ok_or_else(|| Error::invalid("native stream identity/content is unavailable"))
}

/// Pure codec. Native cursors are relayed verbatim, never parsed or sorted.
pub fn encode_notification(
    facts: &KnownRootTaskStream, notification: &Notification, first_sequence: u64,
) -> Result<Vec<StreamPublish>> {
    if first_sequence == 0 {
        return Err(Error::invalid("stream sequence starts at one"));
    }
    let envelope = &notification.envelope;
    let method = text(envelope, "method")?;
    let p = &envelope["params"];
    // Subscription quota is account/window state, never per-turn tokens/cost.
    if method == "usage/changed" {
        return Ok(Vec::new());
    }
    if !matches!(method, "item/delta" | "item/started" | "item/updated"
        | "item/completed" | "turn/started" | "turn/completed" | "session/tokenUsage")
    {
        return Ok(Vec::new());
    }
    if facts.scope.task_id.is_empty() || facts.scope.operation_id.is_empty()
        || text(p, "sessionId")? != facts.scope.native_session_id
    {
        return Err(Error::new("STREAM_SCOPE_MISMATCH", "notification is outside known Root Task"));
    }
    let cursor = text(p, "viewCursor")?.to_owned();
    let mut payloads = Vec::new();
    let (item_id, item_order) = if method == "session/tokenUsage" {
        if text(p, "turnId")? != facts.scope.turn_id {
            return Err(Error::invalid("usage turn differs from known Root Task"));
        }
        let cumulative = &p["cumulative"];
        let counter = |name: &str| cumulative[name].as_u64()
            .ok_or_else(|| Error::invalid("native cumulative usage unavailable"));
        payloads.push((0, StreamPayload::CumulativeUsage {
            prompt_tokens: counter("promptTokens")?,
            output_tokens: counter("outputTokens")?,
            total_tokens: counter("totalTokens")?,
        }));
        ("@usage".to_owned(), 1_000_000)
    } else if matches!(method, "turn/started" | "turn/completed") {
        if text(p, "turnId")? != facts.scope.turn_id {
            return Err(Error::invalid("lifecycle turn differs from known Root Task"));
        }
        let state = if method == "turn/started" { StreamLifecycle::Started }
            else { lifecycle(p["status"].as_str().unwrap_or("unknown")) };
        payloads.push((0, StreamPayload::Lifecycle { state }));
        ("@turn".to_owned(), 1_000_000)
    } else {
        let known = facts.item.as_ref()
            .ok_or_else(|| Error::new("STREAM_ITEM_UNKNOWN", "known item to Operation facts required"))?;
        if known.turn_id != facts.scope.turn_id {
            return Err(Error::invalid("known item is from another turn"));
        }
        if method == "item/delta" {
            if text(p, "itemId")? != known.item_id {
                return Err(Error::invalid("delta is from another known item"));
            }
            let field = p["field"].as_str().unwrap_or("text");
            let (part, class) = match (known.kind, field) {
                (KnownItemKind::AgentMessage, "text") => (0, TextClass::Assistant),
                (KnownItemKind::ToolCall, "output") => (0, TextClass::ToolProgress),
                (KnownItemKind::Reasoning, field) if field.starts_with("summary.") => {
                    let part = field[8..].parse::<u32>()
                        .ok().filter(|n| *n < 65_535)
                        .ok_or_else(|| Error::invalid("invalid native reasoning part index"))?;
                    (part, TextClass::ReasoningSummary)
                }
                (KnownItemKind::HiddenReasoning, _) => {
                    return unavailable(facts, known, cursor, first_sequence);
                }
                _ => return Ok(Vec::new()),
            };
            payloads.push((part, StreamPayload::Text {
                class, text: p["delta"].as_str()
                    .ok_or_else(|| Error::invalid("native delta is not text"))?.to_owned(),
                final_record: false, revision: None,
            }));
        } else {
            let item = &p["item"];
            if text(item, "itemId")? != known.item_id
                || text(item, "turnId")? != facts.scope.turn_id
            {
                return Err(Error::invalid("full item differs from known Root Task item"));
            }
            let expected_kind = match known.kind {
                KnownItemKind::AgentMessage => "agentMessage",
                KnownItemKind::Reasoning => "reasoning",
                KnownItemKind::ToolCall => "toolCall",
                KnownItemKind::HiddenReasoning => {
                    return unavailable(facts, known, cursor, first_sequence);
                }
            };
            if text(item, "kind")? != expected_kind {
                return Err(Error::invalid("native item kind differs from known fact"));
            }
            let revision = item["revision"].as_u64().filter(|r| *r > 0)
                .ok_or_else(|| Error::invalid("native item revision unavailable"))?;
            let status = text(item, "status")?;
            if method == "item/completed" && status == "inProgress" {
                return Err(Error::invalid("completed item is still marked in progress"));
            }
            let final_record = status != "inProgress";
            if item["truncated"] == true {
                payloads.push((0, StreamPayload::Unavailable { reason: StreamUnavailable::NativeTruncation }));
            } else {
                let mut add = |part, class, value: &str| {
                    payloads.push((part, StreamPayload::Text {
                        class, text: value.to_owned(), final_record, revision: Some(revision),
                    }));
                };
                match known.kind {
                    KnownItemKind::AgentMessage => {
                        if let Some(value) = item["text"].as_str() {
                            add(0, TextClass::Assistant, value);
                        } else {
                            payloads.push((0, StreamPayload::Unavailable {
                                reason: StreamUnavailable::NativeContentUnavailable,
                            }));
                        }
                    }
                    KnownItemKind::ToolCall => {
                        if let Some(output) = item["visibleOutput"].as_str() {
                            add(0, TextClass::ToolProgress, output);
                        } else {
                            payloads.push((0, StreamPayload::Lifecycle { state: lifecycle(text(item, "status")?) }));
                        }
                    }
                    KnownItemKind::Reasoning => {
                        if let Some(parts) = item["summary"].as_array() {
                            if parts.len() > MAX_READ_RECORDS {
                                return Err(Error::invalid("too many reasoning parts"));
                            }
                            for (part, value) in parts.iter().enumerate() {
                                add(part as u32, TextClass::ReasoningSummary, value.as_str()
                                    .ok_or_else(|| Error::invalid("invalid reasoning summary part"))?);
                            }
                        }
                        // Raw committed text is native-supplied only; it is
                        // never inferred from tokens or encrypted payloads.
                        if let Some(value) = item["text"].as_str() {
                            add(65_535, TextClass::ReasoningText, value);
                        }
                        if payloads.is_empty() {
                            payloads.push((0, StreamPayload::Unavailable {
                                reason: StreamUnavailable::HiddenOrEncryptedReasoning,
                            }));
                        }
                    }
                    KnownItemKind::HiddenReasoning => unreachable!(),
                }
            }
        }
        (known.item_id.clone(), known.item_order)
    };
    let mut frames = Vec::with_capacity(payloads.len());
    let mut encoded_bytes = 0;
    for (index, (part_index, payload)) in payloads.into_iter().enumerate() {
        let source_sequence = first_sequence.checked_add(index as u64)
            .filter(|n| *n > 0).ok_or_else(|| Error::invalid("stream sequence exhausted"))?;
        let frame = StreamPublish {
            scope: facts.scope.clone(), source_sequence, native_cursor: cursor.clone(),
            item_id: item_id.clone(), item_order, part_index, payload,
        };
        let size = serde_json::to_vec(&frame)?.len();
        encoded_bytes += size;
        if size > MAX_FRAME_BYTES || encoded_bytes > MAX_READ_BYTES {
            return Err(Error::invalid("native stream logical record exceeds bound"));
        }
        frames.push(frame);
    }
    Ok(frames)
}

fn unavailable(
    facts: &KnownRootTaskStream, item: &KnownRootTaskItem, cursor: String, source_sequence: u64,
) -> Result<Vec<StreamPublish>> {
    let frame = StreamPublish {
        scope: facts.scope.clone(), source_sequence, native_cursor: cursor,
        item_id: item.item_id.clone(), item_order: item.item_order, part_index: 0,
        payload: StreamPayload::Unavailable { reason: StreamUnavailable::HiddenOrEncryptedReasoning },
    };
    if serde_json::to_vec(&frame)?.len() > MAX_FRAME_BYTES {
        return Err(Error::invalid("native stream frame exceeds bound"));
    }
    Ok(vec![frame])
}

fn lifecycle(status: &str) -> StreamLifecycle {
    match status {
        "inProgress" => StreamLifecycle::InProgress,
        "completed" => StreamLifecycle::Completed,
        "failed" => StreamLifecycle::Failed,
        "cancelled" => StreamLifecycle::Cancelled,
        _ => StreamLifecycle::TerminalUnknown,
    }
}

/// Connected producer: only existing module IPC publication, with no observe,
/// event, SDK request, native model command or automatic retry.
/// The owner supplies a separate delivery counter for each exact StreamScope.
pub async fn publish_notification(
    link: &mut ModuleLink, facts: &KnownRootTaskStream,
    notification: &Notification, last_sequence: &mut u64,
) -> Result<usize> {
    let first = last_sequence.checked_add(1)
        .ok_or_else(|| Error::invalid("stream sequence exhausted"))?;
    let frames = encode_notification(facts, notification, first)?;
    let count = frames.len();
    for frame in frames {
        // A lost reply may mean published. Never replay it as a new frame.
        *last_sequence = frame.source_sequence;
        link.publish_stream(&frame).await?;
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn synthetic_msp_delta_final_and_quota_have_exact_known_operation_scope() {
        let facts = KnownRootTaskStream {
            scope: StreamScope { binding_id:"b".into(), generation:1, boot_id:"boot".into(),
                task_id:"task".into(), task_revision:1, attempt_id:"attempt".into(), operation_id:"dispatch".into(),
                native_session_id:"root".into(), turn_id:"turn".into() },
            item:Some(KnownRootTaskItem { item_id:"item".into(), turn_id:"turn".into(),
                item_order:2, kind:KnownItemKind::AgentMessage }),
        };
        let delta = Notification { envelope: json!({"jsonrpc":"2.0","method":"item/delta",
            "params":{"sessionId":"root","itemId":"item","delta":"part","viewCursor":"opaque:A"}}) };
        let frames = encode_notification(&facts, &delta, 1).unwrap();
        assert_eq!(frames[0].scope.operation_id, "dispatch");
        assert_eq!(frames[0].item_order, 2);
        assert!(matches!(frames[0].payload, StreamPayload::Text { final_record:false, .. }));
        let final_item = Notification { envelope:json!({"jsonrpc":"2.0","method":"item/completed",
            "params":{"sessionId":"root","viewCursor":"opaque:B","item":{"itemId":"item",
                "turnId":"turn","kind":"agentMessage","revision":2,"status":"completed","text":"final"}}}) };
        let final_frames = encode_notification(&facts, &final_item, 2).unwrap();
        assert!(matches!(final_frames[0].payload, StreamPayload::Text { final_record:true, revision:Some(2), .. }));
        let mut wrong = delta.envelope.clone();
        wrong["params"]["sessionId"] = json!("foreign");
        assert!(encode_notification(&facts, &Notification { envelope:wrong }, 3).is_err());
        assert!(encode_notification(&facts, &Notification {
            envelope:json!({"method":"usage/changed","params":{"window":{"remaining":5}}}),
        }, 3).unwrap().is_empty());
    }
}
