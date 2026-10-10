use super::{
    Service, SessionScan,
    http::{Data, decode},
    valid_id,
};
use crate::{
    error::{Error, Result},
    model,
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    future::Future,
    time::Duration,
};
use tokio::time::Instant;

const MAX_MEMBERS: usize = 256;
const MAX_PAGES: usize = 64;
const MAX_PENDING: usize = 128;
// The Store gives a complete snapshot 20 seconds. Root identity is checked
// first, outside this optional-read budget, leaving margin for that check and
// the final snapshot assembly without extending the Store deadline.
const OPTIONAL_SNAPSHOT_BUDGET: Duration = Duration::from_secs(8);
const SNAPSHOT_BUDGET_EXHAUSTED: &str = "NATIVE_SNAPSHOT_BUDGET_EXHAUSTED";
/// Durable child-log reads per snapshot. Only tracked children are read:
/// natively active members, members with an unfinished recorded period, and
/// members with a bound non-terminal producer (supplied by the Store).
const MAX_CHILD_LOG_READS: usize = 16;
const MAX_TURNS: usize = 128;
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

struct FamilyRead {
    retained: BTreeMap<String, Value>,
    seen: BTreeSet<String>,
    failures: Vec<Value>,
    enumerated: bool,
}

async fn within_budget<T>(deadline: Instant, future: impl Future<Output = Result<T>>) -> Result<T> {
    tokio::time::timeout_at(deadline, future)
        .await
        .map_err(|_| {
            Error::new(
                SNAPSHOT_BUDGET_EXHAUSTED,
                "optional native snapshot read budget exhausted",
            )
        })?
}

fn safe_failure_code(code: &str) -> &str {
    if !code.is_empty()
        && code.len() <= 64
        && code.as_bytes().first().is_some_and(u8::is_ascii_uppercase)
        && code
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
    {
        code
    } else {
        "NATIVE_SNAPSHOT_FAILURE"
    }
}

fn merge_pending_result(
    member: &str,
    kind: &str,
    result: Result<Data<Vec<Value>>>,
    pending: &mut BTreeMap<(String, String, String), Value>,
    failures: &mut Vec<Value>,
) -> Result<()> {
    match result {
        Ok(data) => {
            let valid = data.data.len() <= MAX_PENDING
                && data.data.iter().all(|item| {
                    item["sessionID"] == member
                        && item["id"].as_str().is_some_and(|id| {
                            valid_id(id, if kind == "form" { "frm_" } else { "per" }).is_ok()
                        })
                        && serde_json::to_vec(item).is_ok_and(|bytes| bytes.len() <= 8192)
                });
            if !valid {
                failures.push(json!({
                    "code":"NATIVE_REQUEST_SCHEMA",
                    "session_id":member,
                    "source":kind
                }));
                return Ok(());
            }
            pending.retain(|(session, old_kind, _), _| session != member || old_kind != kind);
            for item in data.data {
                if pending.len() >= MAX_PENDING {
                    failures.push(json!({"code":"PENDING_REQUEST_LIMIT","source":kind}));
                    break;
                }
                let id = item["id"].as_str().unwrap_or_default().to_owned();
                // Do not log raw native question text; it may contain secrets.
                let fingerprint = model::digest(model::canonical(&item)?.as_bytes());
                pending.insert(
                    (member.to_owned(), kind.to_owned(), id.clone()),
                    json!({
                        "session_id":member,
                        "request_id":id,
                        "kind":kind,
                        "fingerprint":fingerprint,
                        "observed_now":true,
                        "native":crate::redaction::value(item)
                    }),
                );
            }
        }
        Err(error) => failures.push(json!({
            "code":safe_failure_code(&error.code),
            "session_id":member,
            "source":kind
        })),
    }
    Ok(())
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
        || value.get("agent").is_some_and(|agent| {
            agent.as_str().is_none_or(|agent| {
                agent.is_empty()
                    || agent.len() > 256
                    || agent.bytes().any(|byte| byte.is_ascii_control())
            })
        })
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
        "project_id":value["projectID"],"model":model,
        "agent":value.get("agent").and_then(bounded_text),"agent_observed":true,
        "time":{"created":value["time"]["created"],"updated":value["time"]["updated"]},
        // A session-wide last outcome is not evidence about an assigned input/turn.
        "native_session_outcome":value["outcome"].as_str().filter(|s|matches!(*s,"succeeded"|"failed"|"interrupted")),"observed_now":true})
}

async fn read_family(
    service: &Service,
    root: &str,
    deadline: Instant,
    mut retained: BTreeMap<String, Value>,
) -> FamilyRead {
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
                failures.push(json!({"code":"FAMILY_LIMIT","source":"family_enumeration"}));
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
            let page = match within_budget(deadline, async {
                service
                    .get("/api/session", &query)
                    .await
                    .and_then(decode::<Page>)
            })
            .await
            {
                Ok(page) => page,
                Err(error) => {
                    failures.push(json!({
                        "code":safe_failure_code(&error.code),
                        "session_id":parent,
                        "source":"family_enumeration"
                    }));
                    enumerated = false;
                    break;
                }
            };
            if page.data.len() > 100 {
                failures.push(json!({
                    "code":"NATIVE_PAGE_LIMIT",
                    "session_id":parent,
                    "source":"family_enumeration"
                }));
                enumerated = false;
                break;
            }
            let mut invalid = false;
            for child in page.data {
                if validate_session(&child, None).is_err() || child["parentID"] != parent {
                    failures.push(json!({
                        "code":"NATIVE_PARENT_MISMATCH",
                        "session_id":parent,
                        "source":"family_enumeration"
                    }));
                    invalid = true;
                    break;
                }
                let id = child["id"].as_str().unwrap_or_default().to_owned();
                if !seen.insert(id.clone()) {
                    failures.push(json!({
                        "code":"NATIVE_FAMILY_CYCLE",
                        "session_id":parent,
                        "source":"family_enumeration"
                    }));
                    invalid = true;
                    break;
                }
                if seen.len() > MAX_MEMBERS {
                    invalid = true;
                    failures.push(json!({"code":"FAMILY_LIMIT","source":"family_enumeration"}));
                    break;
                }
                if retained.len() >= MAX_MEMBERS && !retained.contains_key(&id) {
                    invalid = true;
                    failures.push(json!({
                        "code":"FAMILY_RETAINED_LIMIT",
                        "source":"family_enumeration"
                    }));
                    break;
                }
                let mut entry = compact(&child);
                if let Some(old) = retained.get(&id) {
                    for key in ["execution_scan", "last_turn", "execution_disposition"] {
                        if old.get(key).is_some_and(|value| !value.is_null()) {
                            entry[key] = old[key].clone();
                        }
                    }
                }
                retained.insert(id.clone(), entry);
                queue.push_back(id);
            }
            if invalid {
                enumerated = false;
                break;
            }
            match page.cursor.next {
                None => break,
                Some(next)
                    if !next.is_empty() && next.len() <= 4096 && cursors.insert(next.clone()) =>
                {
                    cursor = Some(next)
                }
                Some(_) => {
                    failures.push(json!({
                        "code":"NATIVE_CURSOR_CYCLE",
                        "session_id":parent,
                        "source":"family_enumeration"
                    }));
                    enumerated = false;
                    break;
                }
            }
        }
    }
    FamilyRead {
        retained,
        seen,
        failures,
        enumerated,
    }
}

async fn read_active(service: &Service, deadline: Instant) -> Result<BTreeMap<String, Value>> {
    let response = within_budget(deadline, async {
        service
            .get("/api/session/active", &[])
            .await
            .and_then(decode::<Data<BTreeMap<String, Value>>>)
    })
    .await?;
    if response
        .data
        .iter()
        .all(|(id, status)| valid_id(id, "ses").is_ok() && status["type"] == "running")
    {
        Ok(response.data)
    } else {
        Err(Error::new(
            "NATIVE_ACTIVE_SCHEMA",
            "native active-session response does not match the selected V2 contract",
        ))
    }
}

struct LifecycleRead {
    retained: BTreeMap<String, Value>,
    failures: Vec<Value>,
    active: Option<BTreeMap<String, Value>>,
    pending: BTreeMap<(String, String, String), Value>,
    turns: Vec<Value>,
    enumerated: bool,
}

struct ChildLogRead {
    retained: BTreeMap<String, Value>,
    failures: Vec<Value>,
    turns: Vec<Value>,
}

async fn read_child_logs(
    service: &Service,
    root_info: &Value,
    mut retained: BTreeMap<String, Value>,
    active: Option<&BTreeMap<String, Value>>,
    bound_children: &BTreeSet<String>,
    deadline: Instant,
) -> ChildLogRead {
    let mut tracked = Vec::new();
    {
        let mut enqueue = |id: &str| {
            if retained.contains_key(id) && !tracked.iter().any(|tracked| tracked == id) {
                tracked.push(id.to_owned());
            }
        };
        for id in bound_children {
            enqueue(id);
        }
        for (id, entry) in &retained {
            if let Some(saved) = entry.get("execution_scan").filter(|value| !value.is_null()) {
                let parent = entry["parentSessionId"].as_str().unwrap_or_default();
                match SessionScan::restore(id, parent, Some(saved)) {
                    Ok(scan) if !scan.is_terminal() => enqueue(id),
                    Err(_) => enqueue(id),
                    Ok(_) => {}
                }
            }
        }
        if let Some(active) = active {
            for id in active.keys() {
                enqueue(id);
            }
        }
    }

    let project_id = root_info["projectID"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    let mut failures = Vec::new();
    let mut turns = Vec::new();
    for (index, id) in tracked.into_iter().enumerate() {
        if index >= MAX_CHILD_LOG_READS {
            failures.push(json!({
                "code":"CHILD_LOG_TRACK_LIMIT",
                "session_id":id,
                "source":"child_execution_log"
            }));
            continue;
        }
        let Some(entry) = retained.get(&id) else {
            continue;
        };
        let parent = entry["parentSessionId"]
            .as_str()
            .unwrap_or_default()
            .to_owned();
        let saved = entry
            .get("execution_scan")
            .filter(|value| !value.is_null())
            .cloned();
        match within_budget(
            deadline,
            service.read_session_execution(&id, &parent, &project_id, saved.as_ref()),
        )
        .await
        {
            Ok(read) if read.synced => {
                let last = read.scan.last_turn();
                let disposition = read.scan.disposition();
                turns.extend(read.scan.turns());
                let entry = retained.get_mut(&id).expect("tracked member was retained");
                entry["execution_scan"] = json!(read.scan);
                if let Some(last) = last {
                    entry["last_turn"] = last;
                }
                entry["execution_disposition"] = json!(disposition);
            }
            Ok(read) => failures.push(json!({
                "code":safe_failure_code(read.gap.unwrap_or("NATIVE_LOG_NOT_SYNCED")),
                "session_id":id,
                "source":"child_execution_log"
            })),
            Err(error) => failures.push(json!({
                "code":safe_failure_code(&error.code),
                "session_id":id,
                "source":"child_execution_log"
            })),
        }
    }
    if turns.len() > MAX_TURNS {
        turns.drain(..turns.len() - MAX_TURNS);
    }
    ChildLogRead {
        retained,
        failures,
        turns,
    }
}

struct PendingRead {
    pending: BTreeMap<(String, String, String), Value>,
    failures: Vec<Value>,
}

async fn read_pending_requests(
    service: &Service,
    root: &str,
    previous: &Value,
    seen: &BTreeSet<String>,
    deadline: Instant,
) -> Result<PendingRead> {
    let mut pending = BTreeMap::new();
    let mut failures = Vec::new();
    // A failed member read or incomplete family listing is not cancellation
    // evidence. Prior pending requests remain visible until a valid exact read
    // for their session and kind replaces them.
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
    if seen.len() > 64 {
        failures.push(json!({"code":"PENDING_MEMBER_LIMIT"}));
    }
    let mut members = Vec::from([root.to_owned()]);
    members.extend(
        seen.iter()
            .filter(|id| id.as_str() != root)
            .take(63)
            .cloned(),
    );
    for member in members {
        let form = read_pending(service, &member, "form", deadline);
        let permission = read_pending(service, &member, "permission", deadline);
        let (form, permission) = tokio::join!(form, permission);
        merge_pending_result(&member, "form", form, &mut pending, &mut failures)?;
        merge_pending_result(
            &member,
            "permission",
            permission,
            &mut pending,
            &mut failures,
        )?;
    }
    Ok(PendingRead { pending, failures })
}

async fn read_lifecycle(
    service: &Service,
    root: &str,
    root_info: &Value,
    previous: &Value,
    bound_children: &BTreeSet<String>,
    deadline: Instant,
) -> Result<LifecycleRead> {
    let mut old_children = BTreeMap::new();
    for old in previous["observed_children"]
        .as_array()
        .into_iter()
        .flatten()
        .take(MAX_MEMBERS)
    {
        if let Some(id) = old["sessionId"]
            .as_str()
            .filter(|id| valid_id(id, "ses").is_ok())
        {
            let mut old = old.clone();
            old["observed_now"] = json!(false);
            old_children.insert(id.to_owned(), old);
        }
    }
    let (family, active_result) = tokio::join!(
        read_family(service, root, deadline, old_children),
        read_active(service, deadline)
    );
    let (active, active_failure) = match active_result {
        Ok(active) => (Some(active), None),
        Err(error) => (None, Some(safe_failure_code(&error.code).to_owned())),
    };
    let FamilyRead {
        retained,
        seen,
        mut failures,
        enumerated,
    } = family;
    let active_failures = active_failure
        .map(|code| {
            json!({
                "code":code,
                "session_id":root,
                "source":"active_drains"
            })
        })
        .into_iter()
        .collect::<Vec<_>>();

    // Once family and active axes resolve, child logs and pending requests are
    // sibling scans under the same deadline. A held log cannot starve a fast
    // form/permission read, and slow catalog axes run outside this pipeline.
    let child_logs = read_child_logs(
        service,
        root_info,
        retained,
        active.as_ref(),
        bound_children,
        deadline,
    );
    let pending_requests = read_pending_requests(service, root, previous, &seen, deadline);
    let (child_logs, pending_requests) = tokio::join!(child_logs, pending_requests);
    let pending_requests = pending_requests?;

    failures.extend(active_failures);
    failures.extend(child_logs.failures);
    failures.extend(pending_requests.failures);
    Ok(LifecycleRead {
        retained: child_logs.retained,
        failures,
        active,
        pending: pending_requests.pending,
        turns: child_logs.turns,
        enumerated,
    })
}

async fn read_pending(
    service: &Service,
    member: &str,
    kind: &str,
    deadline: Instant,
) -> Result<Data<Vec<Value>>> {
    let path = format!("/api/session/{member}/{kind}");
    within_budget(deadline, async {
        service
            .get(&path, &[])
            .await
            .and_then(decode::<Data<Vec<Value>>>)
    })
    .await
}

impl Service {
    pub(crate) async fn snapshot(
        &self,
        root: &str,
        previous: &Value,
        bound_children: &BTreeSet<String>,
    ) -> Result<Snapshot> {
        self.snapshot_with_budget(root, previous, bound_children, OPTIONAL_SNAPSHOT_BUDGET)
            .await
    }

    async fn snapshot_with_budget(
        &self,
        root: &str,
        previous: &Value,
        bound_children: &BTreeSet<String>,
        budget: Duration,
    ) -> Result<Snapshot> {
        // Identity and the root session are mandatory. The optional budget
        // begins only after this exact root read succeeds.
        let root_info = self.session(root).await?;
        let deadline = Instant::now() + budget;
        let lifecycle = read_lifecycle(self, root, &root_info, previous, bound_children, deadline);
        let instructions = within_budget(deadline, self.instruction_entries(root));
        let agent = within_budget(deadline, self.agent_observation(&root_info));
        let model_configuration = within_budget(deadline, self.model_observation(&root_info));
        let (lifecycle, instruction_entries, agent_result, model_result) =
            tokio::join!(lifecycle, instructions, agent, model_configuration);
        let mut lifecycle = lifecycle?;

        let configuration = match &instruction_entries {
            Ok(entries) => match Self::instruction_observation_from_entries(entries) {
                Ok(configuration) => {
                    if configuration["complete"] != true {
                        lifecycle.failures.push(json!({
                            "code":"CONFIGURATION_OBSERVATION_LIMIT",
                            "session_id":root,
                            "source":"instruction_entries"
                        }));
                    }
                    configuration
                }
                Err(error) => {
                    lifecycle.failures.push(json!({
                        "code":safe_failure_code(&error.code),
                        "session_id":root,
                        "source":"instruction_entries"
                    }));
                    json!({
                        "complete":false,
                        "owned_entries":[],
                        "revision":null,
                        "value_content_persisted":false,
                        "source":"experimental.session.instructions.entry.list"
                    })
                }
            },
            Err(error) => {
                lifecycle.failures.push(json!({
                    "code":safe_failure_code(&error.code),
                    "session_id":root,
                    "source":"instruction_entries"
                }));
                json!({
                    "complete":false,
                    "owned_entries":[],
                    "revision":null,
                    "value_content_persisted":false,
                    "source":"experimental.session.instructions.entry.list"
                })
            }
        };
        let goal_configuration = match &instruction_entries {
            Ok(entries) => match Self::goal_observation_from_entries(entries) {
                Ok(goal) => goal,
                Err(error) => {
                    lifecycle.failures.push(json!({
                        "code":safe_failure_code(&error.code),
                        "session_id":root,
                        "source":"session_goal"
                    }));
                    json!({
                        "complete":false,
                        "present":null,
                        "status":null,
                        "revision":null,
                        "objective_digest":null,
                        "settings_revision":null,
                        "source":"experimental.session.instructions.entry.list",
                        "objective_content_persisted":false,
                        "continuation_owner":"controller_record",
                        "native_goal_api":false
                    })
                }
            },
            Err(error) => {
                lifecycle.failures.push(json!({
                    "code":safe_failure_code(&error.code),
                    "session_id":root,
                    "source":"session_goal"
                }));
                json!({
                    "complete":false,
                    "present":null,
                    "status":null,
                    "revision":null,
                    "objective_digest":null,
                    "settings_revision":null,
                    "source":"experimental.session.instructions.entry.list",
                    "objective_content_persisted":false,
                    "continuation_owner":"controller_record",
                    "native_goal_api":false
                })
            }
        };
        let agent_configuration = match agent_result {
            Ok(agent) => agent,
            Err(error) => {
                lifecycle.failures.push(json!({
                    "code":safe_failure_code(&error.code),
                    "session_id":root,
                    "source":"session_agent"
                }));
                json!({
                    "complete":false,
                    "agent_id":null,
                    "definition_digest":null,
                    "settings_revision":null,
                    "catalog_revision":null,
                    "raw_definition_persisted":false,
                    "source":"session.get+agent.list"
                })
            }
        };
        let model_configuration = match model_result {
            Ok(model) => model,
            Err(error) => {
                lifecycle.failures.push(json!({
                    "code":safe_failure_code(&error.code),
                    "session_id":root,
                    "source":"session_model"
                }));
                json!({
                    "complete":false,
                    "model":null,
                    "definition_digest":null,
                    "variant_digest":null,
                    "settings_revision":null,
                    "catalog_revision":null,
                    "raw_definition_persisted":false,
                    "source":"session.get+model.list"
                })
            }
        };
        let active_count = lifecycle.active.as_ref().map(|active| {
            lifecycle
                .retained
                .keys()
                .filter(|id| active.contains_key(*id))
                .count()
                + usize::from(active.contains_key(root))
        });
        let family_coverage = json!({
            "members_total":lifecycle.retained.len(),
            "members_observed_now":lifecycle.retained.values().filter(|entry| entry["observed_now"]==true).count(),
            "members_active_verified":lifecycle.active.as_ref().map(|active| lifecycle.retained.keys().filter(|id| active.contains_key(*id)).count()),
            "members_with_execution_evidence":lifecycle.retained.values().filter(|entry| entry["last_turn"].is_object()).count(),
            "members_with_terminal_evidence":lifecycle.retained.values().filter(|entry| matches!(entry["last_turn"]["terminal"].as_str(),Some("completed"|"failed"|"cancelled"))).count(),
            "root_active_verified":lifecycle.active.as_ref().map(|active| active.contains_key(root)),
        });
        Ok(Snapshot {
            state: json!({
                "native_root_id":root,
                "session":compact(&root_info),
                "observed_children":lifecycle.retained.into_values().collect::<Vec<_>>(),
                "pending_requests":lifecycle.pending.into_values().collect::<Vec<_>>(),
                "turns":lifecycle.turns,
                "family_coverage":family_coverage,
                "execution":match active_count {
                    Some(count) if count > 0 => "observed_active",
                    Some(_) => "not_observed_active",
                    None => "unknown"
                },
                "active_drain_count":active_count,
                "configuration":configuration,
                "agent_configuration":agent_configuration,
                "model_configuration":model_configuration,
                "goal_configuration":goal_configuration,
                "native_service_pid":self.pid,
                "native_service_version":self.version,
                "family_completeness":"partial",
                "enumeration_complete":lifecycle.enumerated,
                "completeness_reason":"volatile_non_atomic_pages_are_not_family_terminal_evidence",
                "source":"opencode_v2.http.snapshot",
                "observed_at_ms":model::now_ms()?,
                "gaps":previous["gaps"].as_u64().unwrap_or(0).saturating_add(u64::from(!lifecycle.failures.is_empty())),
                "failures":lifecycle.failures
            }),
        })
    }
}

#[cfg(test)]
#[path = "snapshot_budget_tests.rs"]
mod snapshot_budget_tests;

#[cfg(test)]
mod overflow_scan_snapshot_tests {
    use crate::runtime::opencode_v2::{
        root_id,
        tests::{Fixture, child_events},
    };
    use serde_json::{Value, json};
    use std::collections::BTreeSet;

    #[tokio::test]
    async fn resumed_overflow_scan_advances_native_log_without_terminal_or_family_proof() {
        let fixture = Fixture::new().await;
        let service = fixture.service().await;
        let open = fixture.open(&service).await;
        let root = root_id(&open.binding_id, open.generation);
        let child = "ses_snapshot_overflow_child";
        let run_ids: Vec<String> = (0..17)
            .map(|index| format!("snapshot_overflow_run_{index}"))
            .collect();
        let runs: Vec<(&str, Option<&str>)> =
            run_ids.iter().map(|run| (run.as_str(), None)).collect();
        let events = child_events(child, &root, &runs);
        {
            let mut world = fixture.world.lock().unwrap();
            world.sessions.insert(
                child.into(),
                json!({
                    "id":child,
                    "parentID":root,
                    "projectID":"prj_fixture",
                    "time":{"created":1,"updated":2}
                }),
            );
            world.log_watermarks.insert(child.into(), 18);
            world.logs.insert(child.into(), events.clone());
        }

        let bound = BTreeSet::from([child.to_owned()]);
        let first = service
            .snapshot(&root, &Value::Null, &bound)
            .await
            .expect("the bounded native log is readable");
        let first_child = first.state["observed_children"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["sessionId"] == child)
            .unwrap();
        assert_eq!(first_child["execution_disposition"], "unknown");
        assert_eq!(first_child["execution_scan"]["overflowed"], true);
        assert_eq!(
            first_child["execution_scan"]["periods"]
                .as_array()
                .unwrap()
                .len(),
            16
        );
        assert!(first_child.get("last_turn").is_none_or(Value::is_null));
        assert_eq!(first.state["turns"], json!([]));
        assert_eq!(first.state["family_completeness"], "partial");
        assert_eq!(
            first.state["family_coverage"]["members_with_terminal_evidence"],
            0
        );

        let mut later_events = events;
        later_events.push(json!({
            "id":"evt_snapshot_overflow_late_terminal",
            "type":"session.execution.succeeded",
            "version":1,
            "created":1.0,
            "durable":{"aggregateID":child,"seq":19,"version":1},
            "data":{"sessionID":child}
        }));
        {
            let mut world = fixture.world.lock().unwrap();
            world.log_watermarks.insert(child.into(), 19);
            world.logs.insert(child.into(), later_events);
        }

        let resumed = service
            .snapshot(&root, &first.state, &bound)
            .await
            .expect("the retained overflow checkpoint resumes the native read");
        let resumed_child = resumed.state["observed_children"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["sessionId"] == child)
            .unwrap();
        assert_eq!(resumed_child["execution_disposition"], "unknown");
        assert_eq!(resumed_child["execution_scan"]["overflowed"], true);
        assert_eq!(resumed_child["execution_scan"]["anchor"]["seq"], 19);
        assert_eq!(resumed_child["execution_scan"]["after"], 18);
        assert!(resumed_child.get("last_turn").is_none_or(Value::is_null));
        assert_eq!(resumed.state["turns"], json!([]));
        assert_eq!(resumed.state["family_completeness"], "partial");
        assert_eq!(
            resumed.state["family_coverage"]["members_with_terminal_evidence"],
            0
        );
    }
}
