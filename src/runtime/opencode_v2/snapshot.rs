use super::{
    Service,
    http::{Data, decode},
    valid_id,
};
use crate::{
    error::{Error, Result},
    model,
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

const MAX_MEMBERS: usize = 256;
const MAX_PAGES: usize = 64;
const MAX_PENDING: usize = 128;
#[derive(Deserialize)]
struct Page {
    data: Vec<Value>,
    cursor: Cursor,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    next: Option<String>,
    #[serde(rename = "previous")]
    _previous: Option<String>,
}

pub(crate) struct Snapshot {
    pub state: Value,
}

pub(super) fn validate_session(value: &Value, expected: Option<&str>) -> Result<()> {
    let id = model::text(value, "id")?;
    valid_id(id, "ses")?;
    if expected.is_some_and(|expected| expected != id)
        || value["projectID"]
            .as_str()
            .is_none_or(|s| s.is_empty() || s.len() > 256)
        || value["time"]["created"].as_u64().is_none()
        || value["time"]["updated"].as_u64().is_none()
        || value
            .get("parentID")
            .is_some_and(|v| v.as_str().is_none_or(|s| valid_id(s, "ses").is_err()))
    {
        return Err(Error::new(
            "NATIVE_SCHEMA_ERROR",
            "invalid or mismatched native session",
        ));
    }
    Ok(())
}
fn bounded_text(value: &Value) -> Option<&str> {
    value.as_str().filter(|s| s.len() <= 256)
}
fn compact(value: &Value) -> Value {
    let model = json!({"id":bounded_text(&value["model"]["id"]),"providerID":bounded_text(&value["model"]["providerID"]),"variant":bounded_text(&value["model"]["variant"])});
    json!({"sessionId":value["id"],"parentSessionId":value["parentID"],
        "project_id":value["projectID"],"model":model,"time":{"created":value["time"]["created"],"updated":value["time"]["updated"]},
        // A session-wide last outcome is not evidence about an assigned input/turn.
        "native_session_outcome":value["outcome"].as_str().filter(|s|matches!(*s,"succeeded"|"failed"|"interrupted")),"observed_now":true})
}
impl Service {
    pub(crate) async fn snapshot(&self, root: &str, previous: &Value) -> Result<Snapshot> {
        let root_info = self.session(root).await?;
        let mut retained = BTreeMap::new();
        for old in previous["observed_children"]
            .as_array()
            .into_iter()
            .flatten()
            .take(MAX_MEMBERS)
        {
            if let Some(id) = old["sessionId"]
                .as_str()
                .filter(|s| valid_id(s, "ses").is_ok())
            {
                let mut old = old.clone();
                old["observed_now"] = json!(false);
                retained.insert(id.to_owned(), old);
            }
        }
        let mut queue = VecDeque::from([root.to_owned()]);
        let mut seen = BTreeSet::from([root.to_owned()]);
        let mut failures = Vec::new();
        let mut pages = 0usize;
        let mut enumerated = true;
        while let Some(parent) = queue.pop_front() {
            let mut cursor: Option<String> = None;
            let mut cursors = BTreeSet::new();
            loop {
                if pages == MAX_PAGES || seen.len() >= MAX_MEMBERS {
                    failures.push(json!({"code":"FAMILY_LIMIT"}));
                    enumerated = false;
                    queue.clear();
                    break;
                }
                let mut query = vec![("parentID", parent.clone()), ("limit", "100".into())];
                if let Some(cursor) = &cursor {
                    query.push(("cursor", cursor.clone()));
                } else {
                    query.push(("order", "asc".into()));
                }
                pages += 1;
                let page = match self
                    .get("/api/session", &query)
                    .await
                    .and_then(decode::<Page>)
                {
                    Ok(page) => page,
                    Err(e) => {
                        failures.push(json!({"code":e.code,"session_id":parent}));
                        enumerated = false;
                        break;
                    }
                };
                if page.data.len() > 100 {
                    failures.push(json!({"code":"NATIVE_PAGE_LIMIT","session_id":parent}));
                    enumerated = false;
                    break;
                }
                let mut invalid = false;
                for child in page.data {
                    if validate_session(&child, None).is_err() || child["parentID"] != parent {
                        failures.push(json!({"code":"NATIVE_PARENT_MISMATCH","session_id":parent}));
                        invalid = true;
                        break;
                    }
                    let id = child["id"].as_str().unwrap_or_default().to_owned();
                    if !seen.insert(id.clone()) {
                        failures.push(json!({"code":"NATIVE_FAMILY_CYCLE","session_id":parent}));
                        invalid = true;
                        break;
                    }
                    if seen.len() > MAX_MEMBERS {
                        invalid = true;
                        failures.push(json!({"code":"FAMILY_LIMIT"}));
                        break;
                    }
                    if retained.len() >= MAX_MEMBERS && !retained.contains_key(&id) {
                        invalid = true;
                        failures.push(json!({"code":"FAMILY_RETAINED_LIMIT"}));
                        break;
                    }
                    retained.insert(id.clone(), compact(&child));
                    queue.push_back(id);
                }
                if invalid {
                    enumerated = false;
                    break;
                }
                match page.cursor.next {
                    None => break,
                    Some(next)
                        if !next.is_empty()
                            && next.len() <= 4096
                            && cursors.insert(next.clone()) =>
                    {
                        cursor = Some(next)
                    }
                    Some(_) => {
                        failures.push(json!({"code":"NATIVE_CURSOR_CYCLE","session_id":parent}));
                        enumerated = false;
                        break;
                    }
                }
            }
        }
        let active = match self
            .get("/api/session/active", &[])
            .await
            .and_then(decode::<Data<BTreeMap<String, Value>>>)
        {
            Ok(v)
                if v.data.iter().all(|(id, status)| {
                    valid_id(id, "ses").is_ok() && status["type"] == "running"
                }) =>
            {
                Some(v.data)
            }
            Ok(_) => {
                failures.push(json!({"code":"NATIVE_ACTIVE_SCHEMA"}));
                None
            }
            Err(e) => {
                failures.push(json!({"code":e.code,"source":"active_drains"}));
                None
            }
        };
        let mut pending = BTreeMap::new();
        // Retain unresolved old questions when a member read fails; absence in an
        // incomplete family enumeration is not a native cancellation receipt.
        for old in previous["pending_requests"]
            .as_array()
            .into_iter()
            .flatten()
            .take(MAX_PENDING)
        {
            if let (Some(session), Some(id), Some(kind)) = (
                old["session_id"].as_str(),
                old["request_id"].as_str(),
                old["kind"].as_str(),
            ) {
                let mut old = old.clone();
                old["observed_now"] = json!(false);
                pending.insert((session.to_owned(), kind.to_owned(), id.to_owned()), old);
            }
        }
        let members = std::iter::once(root.to_owned())
            .chain(seen.iter().filter(|s| s.as_str() != root).cloned())
            .take(64);
        for member in members {
            for kind in ["form", "permission"] {
                match self
                    .get(&format!("/api/session/{member}/{kind}"), &[])
                    .await
                    .and_then(decode::<Data<Vec<Value>>>)
                {
                    Ok(data) => {
                        let valid = data.data.len() <= MAX_PENDING
                            && data.data.iter().all(|item| {
                                item["sessionID"] == member
                                    && item["id"].as_str().is_some_and(|id| {
                                        valid_id(id, if kind == "form" { "frm_" } else { "per" })
                                            .is_ok()
                                    })
                                    && serde_json::to_vec(item)
                                        .is_ok_and(|bytes| bytes.len() <= 8192)
                            });
                        if !valid {
                            failures
                                .push(json!({"code":"NATIVE_REQUEST_SCHEMA","session_id":member}));
                            continue;
                        }
                        pending.retain(|(session, old_kind, _), _| {
                            session != &member || old_kind != kind
                        });
                        for item in data.data {
                            if pending.len() >= MAX_PENDING {
                                failures.push(json!({"code":"PENDING_REQUEST_LIMIT"}));
                                break;
                            }
                            let id = item["id"].as_str().unwrap_or_default().to_owned();
                            // Do not log raw native question text; it may contain secrets.
                            let fingerprint = model::digest(model::canonical(&item)?.as_bytes());
                            pending.insert((member.clone(),kind.into(),id.clone()),json!({"session_id":member,"request_id":id,"kind":kind,
                                "fingerprint":fingerprint,"observed_now":true,"native":crate::redaction::value(item)}));
                        }
                    }
                    Err(e) => {
                        failures.push(json!({"code":e.code,"session_id":member,"source":kind}))
                    }
                }
            }
        }
        if seen.len() > 64 {
            failures.push(json!({"code":"PENDING_MEMBER_LIMIT"}));
        }
        let active_count = active.as_ref().map(|a| {
            retained.keys().filter(|id| a.contains_key(*id)).count()
                + usize::from(a.contains_key(root))
        });
        Ok(Snapshot {
            state: json!({"native_root_id":root,"session":compact(&root_info),
            "observed_children":retained.into_values().collect::<Vec<_>>(),"pending_requests":pending.into_values().collect::<Vec<_>>(),
            "execution":match active_count {Some(n) if n>0=>"observed_active",Some(_)=>"not_observed_active",None=>"unknown"},
            "active_drain_count":active_count,"native_service_pid":self.pid,"native_service_version":self.version,
            "family_completeness":"partial","enumeration_complete":enumerated,
            "completeness_reason":"volatile_non_atomic_pages_are_not_family_terminal_evidence",
            "source":"opencode_v2.http.snapshot","observed_at_ms":model::now_ms()?,
            "gaps":previous["gaps"].as_u64().unwrap_or(0).saturating_add(u64::from(!failures.is_empty())),"failures":failures}),
        })
    }
}
