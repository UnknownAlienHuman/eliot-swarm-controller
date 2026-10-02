//! Addressed native background for OpenCode V2: `agent.background` →
//! native `session.background` (`POST /api/session/{sessionID}/background`).
//!
//! Reviewed at OpenCode `4c0d0ff4`: the endpoint takes no payload, answers
//! `204 NoContent`, and is described natively as "Move active foreground
//! backgroundable tools for this session into background observation. Idle
//! requests are a no-op." The native implementation
//! (`packages/core/src/session.ts`, `Session.background`) moves every job
//! currently blocking the session into background observation through the
//! native Job service and, only when at least one job moved, admits a
//! durable synthetic notice into the session naming the backgrounded work
//! (`- {type}: {title-or-id}`). Eligibility is therefore native-declared:
//! this adapter never decides which tools are backgroundable, never selects
//! individual tools (the endpoint has no selection input) and never invents
//! an expected-turn guard the native contract does not have.
//!
//! Evidence ladder, per the module contract: the POST alone is never
//! success. Execute first reads the current foreground tool inventory from
//! the projected message list, sends exactly one background POST, then
//! re-reads. A synthetic notice that was not present before the POST proves
//! the native boundary and names the moved work. No notice and an empty
//! pre-read inventory is the documented native idle no-op. No notice while
//! foreground tools were observed running leaves the outcome unknown: the
//! native job state is not directly readable, so the adapter does not guess
//! that the tools were backgroundable jobs. A lost POST response is
//! `outcome_unknown`; reconciliation repeats only GET/readback and settles
//! from an observed notice — never from its absence, and never by replaying
//! the POST. The native notice carries no operation marker (it is a fixed
//! native text also producible by a user of the native client), so
//! reconciliation attributes an observed notice to the unresolved
//! operation by session presence; the details record that attribution
//! basis explicitly.
//!
//! This operation is not steer (it claims nothing about queued input), not
//! a form/permission reply, not an interrupt (nothing is stopped: the
//! backgrounded work continues), not an idle declaration and not Task
//! cancellation.
use super::{
    Options, Service,
    effects::{failed, outcome},
    http::{Data, decode},
    valid_id,
};
use crate::{
    error::{Error, Result},
    model,
    runtime::{EffectOutcome, RuntimeCommand, RuntimeOutcome},
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

pub(crate) const BACKGROUND_CONTRACT_REVISION: &str = "opencode-background-v1";
/// Exact first line of the synthetic notice the native service admits when
/// a background call moved at least one blocking job (reviewed at
/// `4c0d0ff4`, `packages/core/src/session.ts`). The adapter pins an exact
/// expected service version, so this is contract text, not a heuristic.
const NOTICE_HEADING: &str = "User requested that active blocking work be moved to the background.";
const NOTICE_LIST_HEADING: &str = "Backgrounded work:";
/// One newest timeline page is the current foreground; older pages are
/// history, not blocking work.
const INVENTORY_MESSAGES: usize = 50;
const MAX_RUNNING_TOOLS: usize = 32;
const MAX_NOTICE_ENTRIES: usize = 64;

#[derive(Deserialize)]
struct TimelinePage {
    data: Vec<Value>,
    cursor: PageCursor,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PageCursor {
    next: Option<String>,
    #[serde(rename = "previous")]
    _previous: Option<String>,
}

#[derive(Clone)]
struct Notice {
    id: String,
    source: &'static str,
    backgrounded: Vec<Value>,
}

#[derive(Default)]
struct Foreground {
    running_tools: Vec<Value>,
    tools_truncated: bool,
    inventory_complete: bool,
    session_active: Option<bool>,
    notices: Vec<Notice>,
    notice_ids: BTreeSet<String>,
}

fn schema_error() -> Error {
    Error::new(
        "NATIVE_SCHEMA_ERROR",
        "native response does not match the selected V2 contract",
    )
}

fn validate_timeline_message(message: &Value, session: &str) -> Result<()> {
    let id = model::text(message, "id").map_err(|_| schema_error())?;
    valid_id(id, "msg_").map_err(|_| schema_error())?;
    if message.get("sessionID").is_some_and(|s| s != session)
        || message["time"]["created"]
            .as_f64()
            .is_none_or(|n| !n.is_finite() || n < 0.0)
        || message["type"]
            .as_str()
            .is_none_or(|s| s.is_empty() || s.len() > 64)
    {
        return Err(schema_error());
    }
    Ok(())
}

/// Parse the native background notice text. Returns the backgrounded-work
/// entries when `text` is exactly the native notice shape, else `None`.
/// Entry lines are `- {type}: {title-or-id}`; the label is free native text
/// and is retained as data only.
fn parse_notice(text: &str) -> Option<Vec<Value>> {
    if !text.starts_with(NOTICE_HEADING) {
        return None;
    }
    let mut entries = Vec::new();
    let mut in_list = false;
    for line in text.lines() {
        if line == NOTICE_LIST_HEADING {
            in_list = true;
            continue;
        }
        if !in_list {
            continue;
        }
        let Some(item) = line.strip_prefix("- ") else {
            break;
        };
        let (kind, label) = item.split_once(": ").unwrap_or((item, ""));
        entries.push(json!({"type":kind,"label":label}));
        if entries.len() >= MAX_NOTICE_ENTRIES {
            break;
        }
    }
    Some(entries)
}

fn target_session(command: &RuntimeCommand, root: &str) -> Result<String> {
    // The store validates the admitted field set (`model::validate_mutation`);
    // the adapter reads only the optional addressed target.
    match command.input.get("session_id") {
        None | Some(Value::Null) => Ok(root.to_owned()),
        Some(_) => {
            let session = model::text(&command.input, "session_id")?;
            valid_id(session, "ses")?;
            Ok(session.to_owned())
        }
    }
}

impl Service {
    /// Read the session's current foreground state: running tool parts from
    /// the newest projected message page, pending background notices from
    /// both the projection and the durable inbox, and the native active
    /// map. Read-only; a malformed projection is a schema error, never an
    /// empty healthy inventory.
    async fn foreground_observation(&self, session: &str) -> Result<Foreground> {
        let page: TimelinePage = decode(
            self.get(
                &format!("/api/session/{session}/message"),
                &[
                    ("limit", INVENTORY_MESSAGES.to_string()),
                    ("order", "desc".into()),
                ],
            )
            .await?,
        )?;
        if page.data.len() > INVENTORY_MESSAGES {
            return Err(schema_error());
        }
        let mut foreground = Foreground {
            inventory_complete: page.cursor.next.is_none(),
            ..Foreground::default()
        };
        let mut notices: BTreeMap<String, Notice> = BTreeMap::new();
        for message in &page.data {
            validate_timeline_message(message, session)?;
            if message["type"] == "synthetic"
                && let Some(text) = message["text"].as_str()
                && let Some(backgrounded) = parse_notice(text)
            {
                let id = message["id"].as_str().unwrap_or_default().to_owned();
                notices.entry(id.clone()).or_insert(Notice {
                    id,
                    source: "message_projection",
                    backgrounded,
                });
            }
            if message["type"] != "assistant" {
                continue;
            }
            for part in message["content"].as_array().into_iter().flatten() {
                if part["type"] != "tool"
                    || !matches!(
                        part["state"]["status"].as_str(),
                        Some("running" | "streaming")
                    )
                {
                    continue;
                }
                let tool_id = model::text(part, "id").map_err(|_| schema_error())?;
                let name = model::text(part, "name").map_err(|_| schema_error())?;
                if tool_id.len() > 256 || name.is_empty() || name.len() > 256 {
                    return Err(schema_error());
                }
                if foreground.running_tools.len() >= MAX_RUNNING_TOOLS {
                    foreground.tools_truncated = true;
                    continue;
                }
                foreground.running_tools.push(json!({
                    "message_id":message["id"],
                    "tool_call_id":tool_id,
                    "name":name,
                }));
            }
        }
        let inbox: Data<Vec<Value>> = decode(
            self.get(&format!("/api/session/{session}/inbox"), &[])
                .await?,
        )?;
        for item in &inbox.data {
            let id = model::text(item, "id").map_err(|_| schema_error())?;
            valid_id(id, "msg_").map_err(|_| schema_error())?;
            if item.get("sessionID").is_some_and(|s| s != session)
                || item["type"]
                    .as_str()
                    .is_none_or(|s| s.is_empty() || s.len() > 64)
            {
                return Err(schema_error());
            }
            if item["type"] == "synthetic"
                && let Some(text) = item["payload"]["text"].as_str()
                && let Some(backgrounded) = parse_notice(text)
            {
                notices.entry(id.to_owned()).or_insert(Notice {
                    id: id.to_owned(),
                    source: "inbox",
                    backgrounded,
                });
            }
        }
        foreground.notices = notices.into_values().collect();
        foreground.notice_ids = foreground
            .notices
            .iter()
            .map(|notice| notice.id.clone())
            .collect();
        // The active map is supporting evidence only: an unreadable or
        // malformed map records unknown, it does not block the operation.
        foreground.session_active = match self
            .get("/api/session/active", &[])
            .await
            .and_then(decode::<Data<BTreeMap<String, Value>>>)
        {
            Ok(active)
                if active
                    .data
                    .values()
                    .all(|status| status["type"] == "running") =>
            {
                Some(active.data.contains_key(session))
            }
            _ => None,
        };
        Ok(foreground)
    }

    fn background_details(
        target: &str,
        root: &str,
        before: &Foreground,
        after: Option<&Foreground>,
        condition: &str,
        evidence: &str,
    ) -> Value {
        json!({
            "completion_condition":condition,
            "evidence":evidence,
            "native_endpoint":"session.background",
            "session_id":target,
            "target_is_root":target == root,
            "session_active_before":before.session_active,
            "foreground_tools_before":before.running_tools,
            "foreground_tools_truncated":before.tools_truncated,
            "foreground_inventory_complete":before.inventory_complete,
            "foreground_tools_after":after.map(|after| after.running_tools.clone()),
            "execution_complete":false,
        })
    }

    /// Execute one addressed background operation. The prepare phase proves
    /// binding ownership/generation and reads the foreground inventory
    /// before the single native mutation; the postcondition is the
    /// subsequent observation, never the POST alone.
    pub(super) async fn execute_background(
        &self,
        command: &RuntimeCommand,
        options: &Options,
    ) -> RuntimeOutcome {
        let prepared = async {
            let root = command
                .native_root_id
                .as_deref()
                .ok_or_else(|| Error::invalid("native root is missing"))?;
            let target = target_session(command, root)?;
            self.verify_binding(root, options, &command.binding_id, command.generation)
                .await?;
            if target != root {
                self.owns_member(root, &target).await?;
            }
            let before = self.foreground_observation(&target).await?;
            Ok((root.to_owned(), target, before))
        }
        .await;
        let (root, target, before) = match prepared {
            Ok(value) => value,
            Err(error) => return failed(command, options, &error, false),
        };
        match self
            .post_no_body(&format!("/api/session/{target}/background"))
            .await
        {
            Ok(Value::Null) => {}
            Ok(_) => {
                return failed(
                    command,
                    options,
                    &Error::new("NATIVE_SCHEMA_ERROR", "unexpected background response"),
                    true,
                );
            }
            // The session and its read surfaces were verified immediately
            // before this body-less POST, so a native rejection here can
            // only mean the installed service does not serve the route:
            // report the capability unsupported, never a guessed retry.
            Err(error) if error.code == "NATIVE_REJECTED" => {
                let mut details = Self::background_details(
                    &target,
                    &root,
                    &before,
                    None,
                    "native_background_unsupported",
                    "route_rejected_after_session_verification",
                );
                details["code"] = json!("UNSUPPORTED_CAPABILITY");
                return outcome(command, EffectOutcome::Rejected, options, details);
            }
            Err(error) => return failed(command, options, &error, true),
        }
        let mut after = match self.foreground_observation(&target).await {
            Ok(value) => value,
            Err(error) => {
                return outcome(
                    command,
                    EffectOutcome::Unknown,
                    options,
                    json!({"code":error.code,"completion_condition":"native_background_unproven","evidence":"post_readback_failed"}),
                );
            }
        };
        if after
            .notices
            .iter()
            .all(|notice| before.notice_ids.contains(&notice.id))
            && !before.running_tools.is_empty()
        {
            // The durable notice is admitted before the native response,
            // but its projection can lag one read. One bounded re-read
            // keeps a lagging projection from stranding a proven effect.
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            if let Ok(value) = self.foreground_observation(&target).await {
                after = value;
            }
        }
        if let Some(notice) = after
            .notices
            .iter()
            .find(|notice| !before.notice_ids.contains(&notice.id))
        {
            let mut details = Self::background_details(
                &target,
                &root,
                &before,
                Some(&after),
                "native_foreground_tools_backgrounded",
                "background_notice_readback",
            );
            details["backgrounded"] = json!(notice.backgrounded);
            details["background_notice_id"] = json!(notice.id);
            details["background_notice_source"] = json!(notice.source);
            return outcome(command, EffectOutcome::Applied, options, details);
        }
        if before.running_tools.is_empty() {
            // The documented native idle no-op: the boundary answered 204
            // and the subsequent observation is consistent with it — no
            // foreground tools were running and no notice was admitted.
            let mut details = Self::background_details(
                &target,
                &root,
                &before,
                Some(&after),
                "native_background_noop",
                "post_readback",
            );
            details["backgrounded"] = json!([]);
            return outcome(command, EffectOutcome::Applied, options, details);
        }
        // Foreground tools were observed running but no notice proves any
        // of them was a backgroundable blocking job. The native job state
        // is not directly readable; stay unknown rather than guess.
        let mut details = Self::background_details(
            &target,
            &root,
            &before,
            Some(&after),
            "native_background_unproven",
            "post_readback",
        );
        details["code"] = json!("NATIVE_BACKGROUND_UNPROVEN");
        outcome(command, EffectOutcome::Unknown, options, details)
    }

    /// Readback-only reconciliation: an observed native notice settles the
    /// operation as backgrounded; its absence proves nothing (the original
    /// POST may never have landed, or may have been an idle no-op) and the
    /// operation stays unknown. The POST is never replayed.
    pub(super) async fn reconcile_background(
        &self,
        command: &RuntimeCommand,
        options: &Options,
    ) -> RuntimeOutcome {
        let readback: Result<(String, String, Foreground)> = async {
            let root = command
                .native_root_id
                .as_deref()
                .ok_or_else(|| Error::invalid("native root is missing"))?;
            let target = target_session(command, root)?;
            self.verify_binding(root, options, &command.binding_id, command.generation)
                .await?;
            if target != root {
                self.owns_member(root, &target).await?;
            }
            let observed = self.foreground_observation(&target).await?;
            Ok((root.to_owned(), target, observed))
        }
        .await;
        let (root, target, observed) = match readback {
            Ok(value) => value,
            Err(error) => {
                return outcome(
                    command,
                    EffectOutcome::Unknown,
                    options,
                    json!({"code":error.code}),
                );
            }
        };
        if let Some(notice) = observed.notices.first() {
            let mut details = Self::background_details(
                &target,
                &root,
                &observed,
                Some(&observed),
                "native_foreground_tools_backgrounded",
                "background_notice_readback",
            );
            details["backgrounded"] = json!(notice.backgrounded);
            details["background_notice_id"] = json!(notice.id);
            details["background_notice_source"] = json!(notice.source);
            // The native notice has no operation marker: presence on this
            // exact session is the attribution basis, recorded as such.
            details["attribution"] = json!("session_notice_presence_no_operation_marker");
            return outcome(command, EffectOutcome::Applied, options, details);
        }
        outcome(
            command,
            EffectOutcome::Unknown,
            options,
            json!({"code":"NATIVE_EVIDENCE_UNAVAILABLE","completion_condition":"native_background_unproven","evidence":"notice_absent_at_readback","session_id":target}),
        )
    }
}
