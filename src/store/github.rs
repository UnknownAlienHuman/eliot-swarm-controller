//! Optional GitHub Issue readback and Task work-pool integration.
//!
//! All network reads use the installed `gh` account outside the Store
//! transaction. The Store retains each poll as an ordinary Operation before
//! I/O, commits source facts and Task mappings atomically, and retains safe
//! failure coverage without interpreting a failed read as an empty source.

use super::{Store, current_principal, gm, mutate, mutate_in_transaction, operations, tasks};
use crate::{
    config::Config,
    error::{Error, Result},
    github::{
        client::{GhCli, IssueReadback, RepositoryRef},
        observer::{self, IssueSnapshot},
        protocol::{self, SourcePollRequest, SourceSetupRequest},
        work_pool,
    },
    model::{self, Principal, Role, TaskSpec},
};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde_json::{Value, json};

#[derive(Debug, Clone)]
struct Source {
    source_id: String,
    project_id: String,
    host: String,
    owner: String,
    repo: String,
    repository_id: i64,
    next_page: u32,
    poll_generation: i64,
}

#[derive(Debug, Clone)]
struct Mapping {
    task_id: Option<String>,
    status: String,
    applied_spec_digest: Option<String>,
    error: Option<Value>,
    created: bool,
    revised: bool,
}

#[derive(Debug, Clone)]
struct PriorMapping {
    current_fact_digest: String,
    current_event_key: String,
    task_id: Option<String>,
    applied_task_spec_digest: Option<String>,
}

struct TaskMappingContext<'a> {
    principal: &'a Principal,
    source: &'a Source,
    config: &'a Config,
    now: i64,
}

struct TaskMappingProjection<'a> {
    issue: &'a IssueReadback,
    spec: &'a TaskSpec,
    spec_digest: &'a str,
    prior: Option<&'a PriorMapping>,
    event_key: &'a str,
}

impl Store {
    pub(super) async fn github_call(
        &self,
        principal: Principal,
        method: String,
        params: Value,
    ) -> Result<Value> {
        if matches!(
            method.as_str(),
            "github.source.inspect" | "github.source.get" | "github.work_pool.preview"
        ) {
            let value = protocol::validate_read(&method, &params)?;
            return match method.as_str() {
                "github.source.inspect" => {
                    let p = principal.clone();
                    self.run(move |db| {
                        let current = current_principal(db, p)?;
                        authorize_source_reader(db, &current)
                    })
                    .await?;
                    let request = protocol::SourceInspectRequest::parse(&value)?;
                    let repository =
                        RepositoryRef::new(&request.host, &request.owner, &request.repo)?;
                    let readback = GhCli.repository(&repository).await?;
                    Ok(json!({
                        "host":repository.host,
                        "owner":repository.owner,
                        "repo":repository.name,
                        "repository_id":readback.id,
                        "full_name":readback.full_name,
                        "html_url":readback.html_url,
                        "transport":"installed_gh_cli_existing_account",
                        "credentials_read_by_store":false
                    }))
                }
                "github.source.get" => {
                    let request = protocol::SourceReadRequest::parse(&value)?;
                    let p = principal.clone();
                    self.run(move |db| {
                        let current = current_principal(db, p)?;
                        authorize_source_reader(db, &current)?;
                        let source = load_source(db, &request.source_id)?;
                        source_readback(db, &source)
                    })
                    .await
                }
                "github.work_pool.preview" => {
                    let request = protocol::WorkPoolPreviewRequest::parse(&value)?;
                    let p = principal.clone();
                    self.run(move |db| {
                        let current = current_principal(db, p)?;
                        authorize_source_reader(db, &current)?;
                        let source = load_source(db, &request.source_id)?;
                        pool_preview(db, &source, &request)
                    })
                    .await
                }
                _ => unreachable!("closed GitHub read method list"),
            };
        }

        let value = protocol::validate_mutation(&method, &params)?;
        match method.as_str() {
            "github.source.setup" => {
                let request = SourceSetupRequest::parse(&value)?;
                protocol::validate_source_id(&request.source_id)?;
                protocol::validate_project_id(&request.project_id)?;
                let repository = RepositoryRef::new(&request.host, &request.owner, &request.repo)?;
                validate_project_binding(&self.config, &request, &repository)?;

                // Exact retries return their retained receipt even if GitHub
                // has since become unavailable. A new setup always verifies
                // the immutable repository ID before registering the source.
                let p = principal.clone();
                let request_value = value.clone();
                let existing = self
                    .run(move |db| {
                        let current = current_principal(db, p)?;
                        current.require_operator()?;
                        request_exists(db, &current, "github.source.setup", &request_value)
                    })
                    .await?;
                if !existing {
                    let repository_readback = GhCli.repository(&repository).await?;
                    if repository_readback.id != request.repository_id {
                        return Err(Error::new(
                            "GITHUB_REPOSITORY_IDENTITY_CHANGED",
                            "repository_id does not match the selected GitHub path",
                        ));
                    }
                }
                let config = self.config.clone();
                self.run(move |db| {
                    let current = current_principal(db, principal)?;
                    current.require_operator()?;
                    mutate(db, &current, "github.source.setup", &value, &config)
                })
                .await
            }
            "github.source.poll" => self.github_poll(principal, value).await,
            "github.work_pool.apply" => {
                let config = self.config.clone();
                self.run(move |db| {
                    let current = current_principal(db, principal)?;
                    mutate(db, &current, "github.work_pool.apply", &value, &config)
                })
                .await
            }
            _ => Err(Error::new("METHOD_NOT_FOUND", method)),
        }
    }

    async fn github_poll(&self, principal: Principal, value: Value) -> Result<Value> {
        let request = SourcePollRequest::parse(&value)?;
        protocol::validate_source_id(&request.source_id)?;
        let config = self.config.clone();
        let admission_params = value.clone();
        let admission_principal = principal.clone();
        let (admission, current_operation) = self
            .run(move |db| {
                let current = current_principal(db, admission_principal)?;
                current.require_operator()?;
                let receipt = mutate(
                    db,
                    &current,
                    "github.source.poll",
                    &admission_params,
                    &config,
                )?;
                let operation_id = model::text(&receipt, "operation_id")?.to_owned();
                let operation = operations::get_operation(db, &operation_id)?;
                Ok((receipt, operation))
            })
            .await?;
        let operation_id = model::text(&admission, "operation_id")?.to_owned();
        match current_operation["state"].as_str() {
            Some("settled") => return Ok(current_operation["result"].clone()),
            Some("rejected") => return Err(operation_error(&current_operation["result"])),
            Some("cancelled") => {
                let p = principal.clone();
                let source_id = request.source_id.clone();
                let operation_id = operation_id.clone();
                self.run(move |db| cleanup_cancelled_poll(db, &p, &source_id, &operation_id))
                    .await?;
                return Err(Error::new(
                    "GITHUB_POLL_CANCELLED",
                    "the retained GitHub poll was cancelled before its source read",
                ));
            }
            Some("queued") => {}
            Some("outcome_unknown") => {}
            Some("sending") => {
                return Err(Error::new(
                    "GITHUB_POLL_IN_PROGRESS",
                    "the retained GitHub poll is already reading; inspect github.source.get",
                ));
            }
            _ => {
                return Err(Error::new(
                    "GITHUB_POLL_NOT_RESUMABLE",
                    "the retained GitHub poll Operation is not in a resumable read state",
                ));
            }
        }

        if matches!(
            current_operation["state"].as_str(),
            Some("queued" | "outcome_unknown")
        ) {
            let p = principal.clone();
            let source_id = request.source_id.clone();
            let operation_id = operation_id.clone();
            self.run(move |db| begin_poll_read(db, &p, &source_id, &operation_id))
                .await?;
        }

        let plan_request = request.clone();
        let plan_operation = operation_id.clone();
        let plan = self
            .run(move |db| poll_plan(db, &plan_request.source_id, &plan_operation))
            .await?;
        let repository = RepositoryRef::new(&plan.host, &plan.owner, &plan.repo)?;
        let snapshot =
            observer::read_issue_snapshot(&GhCli, &repository, plan.repository_id, plan.next_page)
                .await;
        let snapshot = match snapshot {
            Ok(snapshot) => snapshot,
            Err(error) => {
                let p = principal.clone();
                let source_id = request.source_id.clone();
                let operation_id = operation_id.clone();
                let failure = error.clone();
                self.run(move |db| {
                    let current = current_principal(db, p)?;
                    current.require_operator()?;
                    finish_poll_failure(db, &current, &source_id, &operation_id, &failure)
                })
                .await?;
                return Err(error);
            }
        };

        let config = self.config.clone();
        let source_id = request.source_id.clone();
        let operation_id_for_commit = operation_id.clone();
        let p = principal.clone();
        let committed = self
            .run(move |db| {
                let current = current_principal(db, p)?;
                current.require_operator()?;
                persist_snapshot(
                    db,
                    &current,
                    &source_id,
                    &operation_id_for_commit,
                    &snapshot,
                    &config,
                )
            })
            .await;
        if committed.is_err() {
            // Never return a success-shaped empty page if a local projection
            // could not be committed. Preserve a safe source-level report if
            // the Store remains writable; keep low-level database detail
            // outside the public projection.
            let safe_error = Error::new(
                "GITHUB_POLL_COMMIT_FAILED",
                "the bounded GitHub read could not be committed; inspect github.source.get",
            );
            let p = principal.clone();
            let source_id = request.source_id.clone();
            let operation_id = operation_id.clone();
            let failure = safe_error.clone();
            let _ = self
                .run(move |db| {
                    let current = current_principal(db, p)?;
                    current.require_operator()?;
                    finish_poll_failure(db, &current, &source_id, &operation_id, &failure)
                })
                .await;
            return Err(safe_error);
        }
        let read_id = operation_id.clone();
        self.run(move |db| operations::get_operation(db, &read_id))
            .await
            .map(|operation| operation["result"].clone())
    }
}

/// Local mutation half of the GitHub methods. Network verification has
/// already happened before setup enters this transaction; poll is queued and
/// completed by `github_poll` outside this function.
pub(super) fn apply(
    tx: &Transaction<'_>,
    principal: &Principal,
    method: &str,
    value: &Value,
    config: &Config,
    operation_id: &str,
    now: i64,
) -> Result<(Value, bool)> {
    match method {
        "github.source.setup" => {
            apply_source_setup(tx, principal, value, config, operation_id, now)
                .map(|value| (value, false))
        }
        "github.source.poll" => {
            let request = SourcePollRequest::parse(value)?;
            protocol::validate_source_id(&request.source_id)?;
            principal.require_operator()?;
            load_source(tx, &request.source_id)?;
            let active: Option<String> = tx
                .query_row(
                    "SELECT operation_id FROM github_poll_leases WHERE source_id=?1",
                    [&request.source_id],
                    |row| row.get(0),
                )
                .optional()?;
            if let Some(active_operation) = active {
                if active_operation != operation_id {
                    return Err(Error::conflict(
                        "another GitHub poll is retained for this source; read it or resume its exact request",
                    ));
                }
            } else {
                tx.execute(
                    "INSERT INTO github_poll_leases(source_id,operation_id,started_at_ms) VALUES(?1,?2,?3)",
                    params![request.source_id, operation_id, now],
                )?;
                tx.execute(
                    "UPDATE github_sources SET last_poll_status='polling',last_poll_error_json=NULL,last_poll_operation_id=?2,last_poll_started_at_ms=?3,last_poll_finished_at_ms=NULL,updated_at_ms=?3 WHERE source_id=?1",
                    params![request.source_id, operation_id, now],
                )?;
            }
            Ok((
                json!({
                    "operation_id":operation_id,
                    "source_id":request.source_id,
                    "outcome":"poll_queued",
                    "current_state_read_method":"github.source.get"
                }),
                true,
            ))
        }
        "github.work_pool.apply" => {
            let request = protocol::WorkPoolApplyRequest::parse(value)?;
            protocol::validate_source_id(&request.source_id)?;
            apply_pool_selection(tx, principal, &request, operation_id, now, config)
                .map(|value| (value, false))
        }
        _ => Err(Error::new("METHOD_NOT_FOUND", method)),
    }
}

fn apply_source_setup(
    tx: &Transaction<'_>,
    principal: &Principal,
    value: &Value,
    config: &Config,
    operation_id: &str,
    now: i64,
) -> Result<Value> {
    principal.require_operator()?;
    let request = SourceSetupRequest::parse(value)?;
    protocol::validate_source_id(&request.source_id)?;
    protocol::validate_project_id(&request.project_id)?;
    let repository = RepositoryRef::new(&request.host, &request.owner, &request.repo)?;
    validate_project_binding(config, &request, &repository)?;

    let same_id: Option<(String, i64, String)> = tx
        .query_row(
            "SELECT host,repository_id,project_id FROM github_sources WHERE source_id=?1",
            [&request.source_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    if let Some((host, repository_id, project_id)) = same_id
        && (host != repository.host
            || repository_id != request.repository_id
            || project_id != request.project_id)
    {
        return Err(Error::conflict(
            "source_id is already bound to a different immutable repository or project identity",
        ));
    }
    let existing_source: Option<String> = tx
        .query_row(
            "SELECT source_id FROM github_sources WHERE host=?1 AND repository_id=?2",
            params![repository.host, request.repository_id],
            |row| row.get(0),
        )
        .optional()?;
    if existing_source
        .as_deref()
        .is_some_and(|source_id| source_id != request.source_id)
    {
        return Err(Error::conflict(
            "repository identity is already registered under another source_id",
        ));
    }

    tx.execute(
        "INSERT INTO github_sources(source_id,project_id,host,owner,repository_name,repository_id,next_page,poll_generation,last_poll_status,last_coverage_json,created_by,created_at_ms,updated_at_ms) VALUES(?1,?2,?3,?4,?5,?6,1,0,'never','{}',?7,?8,?8) ON CONFLICT(source_id) DO UPDATE SET project_id=excluded.project_id,owner=excluded.owner,repository_name=excluded.repository_name,updated_at_ms=excluded.updated_at_ms",
        params![
            request.source_id,
            request.project_id,
            repository.host,
            repository.owner,
            repository.name,
            request.repository_id,
            principal.client_id,
            now
        ],
    )?;
    Ok(json!({
        "operation_id":operation_id,
        "source_id":request.source_id,
        "project_id":request.project_id,
        "repository_id":request.repository_id,
        "host":repository.host,
        "owner":repository.owner,
        "repo":repository.name,
        "status":"configured",
        "initial_page":1,
        "external_mutations_enabled":false
    }))
}

fn apply_pool_selection(
    tx: &Transaction<'_>,
    principal: &Principal,
    request: &protocol::WorkPoolApplyRequest,
    operation_id: &str,
    now: i64,
    _config: &Config,
) -> Result<Value> {
    let source = load_source(tx, &request.source_id)?;
    authorize_source_reader(tx, principal)?;
    for task_id in &request.task_ids {
        let project_id: Option<String> = tx
            .query_row(
                "SELECT t.project_id FROM github_work_pool_members m JOIN tasks t ON t.task_id=m.task_id WHERE m.source_id=?1 AND m.task_id=?2",
                params![request.source_id, task_id],
                |row| row.get(0),
            )
            .optional()?;
        let project_id = project_id.ok_or_else(|| {
            Error::invalid("every selected Task must already belong to this source work pool")
        })?;
        authorize_task_scope(tx, principal, task_id, &project_id)?;
    }
    tx.execute(
        "UPDATE github_work_pool_members SET selected=0,selection_order=NULL,updated_at_ms=?2 WHERE source_id=?1",
        params![request.source_id, now],
    )?;
    for (selection_order, task_id) in request.task_ids.iter().enumerate() {
        tx.execute(
            "UPDATE github_work_pool_members SET selected=1,selection_order=?3,updated_at_ms=?4 WHERE source_id=?1 AND task_id=?2",
            params![request.source_id, task_id, selection_order as i64, now],
        )?;
    }
    Ok(json!({
        "operation_id":operation_id,
        "source_id":source.source_id,
        "selected_task_ids":request.task_ids,
        "selected_count":request.task_ids.len(),
        "dispatch_started":false,
        "read_method":"github.work_pool.preview"
    }))
}

fn validate_project_binding(
    config: &Config,
    request: &SourceSetupRequest,
    repository: &RepositoryRef,
) -> Result<()> {
    let project = config
        .forge
        .projects
        .get(&request.project_id)
        .ok_or_else(|| {
            Error::new(
                "FORGE_PROJECT_UNCONFIGURED",
                "GitHub source project must already have an explicit local forge mapping",
            )
        })?;
    let requested = crate::forge::canonical_repository(&format!(
        "{}/{}/{}",
        repository.host, repository.owner, repository.name
    ))?;
    let configured = crate::forge::canonical_repository(&project.canonical_repository)?;
    if requested != configured {
        return Err(Error::new(
            "GITHUB_PROJECT_BINDING_MISMATCH",
            "GitHub source path does not match the local project repository mapping",
        ));
    }
    Ok(())
}

fn authorize_source_reader(db: &Connection, principal: &Principal) -> Result<()> {
    match &principal.role {
        Role::Operator => Ok(()),
        Role::Manager => gm::require_authority(db, principal),
        _ => Err(Error::new(
            "FORBIDDEN",
            "GitHub source readback requires the local operator or current GM",
        )),
    }
}

fn authorize_task_scope(
    db: &Connection,
    principal: &Principal,
    task_id: &str,
    project_id: &str,
) -> Result<()> {
    match &principal.role {
        Role::Operator => Ok(()),
        Role::Manager
            if crate::automation::authorization::current_manager_has_task_scope(
                db, principal, task_id, project_id,
            )? =>
        {
            Ok(())
        }
        _ => Err(Error::new(
            "FORBIDDEN",
            "the current manager has no scope for every selected GitHub Task",
        )),
    }
}

fn load_source(db: &Connection, source_id: &str) -> Result<Source> {
    protocol::validate_source_id(source_id)?;
    db.query_row(
        "SELECT source_id,project_id,host,owner,repository_name,repository_id,next_page,poll_generation FROM github_sources WHERE source_id=?1",
        [source_id],
        |row| {
            let next_page: i64 = row.get(6)?;
            Ok(Source {
                source_id: row.get(0)?,
                project_id: row.get(1)?,
                host: row.get(2)?,
                owner: row.get(3)?,
                repo: row.get(4)?,
                repository_id: row.get(5)?,
                next_page: u32::try_from(next_page).unwrap_or(0),
                poll_generation: row.get(7)?,
            })
        },
    )
    .optional()?
    .filter(|source| source.next_page > 0)
    .ok_or_else(|| Error::new("NOT_FOUND", format!("GitHub source {source_id}")))
}

fn request_exists(
    db: &Connection,
    principal: &Principal,
    method: &str,
    value: &Value,
) -> Result<bool> {
    let request_id = model::text(value, "client_request_id")?;
    let existing: Option<(String, String)> = db
        .query_row(
            "SELECT method,original_request_json FROM operations WHERE caller_id=?1 AND client_request_id=?2",
            params![principal.client_id, request_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((old_method, original)) = existing else {
        return Ok(false);
    };
    if old_method != method || original != model::canonical(value)? {
        return Err(Error::new(
            "REQUEST_ID_CONFLICT",
            "request ID was used with a different method or payload",
        ));
    }
    Ok(true)
}

fn source_readback(db: &Connection, source: &Source) -> Result<Value> {
    let (status, error_json, coverage_json, operation, started, finished): (
        String,
        Option<String>,
        String,
        Option<String>,
        Option<i64>,
        Option<i64>,
    ) = db.query_row(
        "SELECT last_poll_status,last_poll_error_json,last_coverage_json,last_poll_operation_id,last_poll_started_at_ms,last_poll_finished_at_ms FROM github_sources WHERE source_id=?1",
        [&source.source_id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?)),
    )?;
    let (issues, mapped, unresolved, selected): (i64, i64, i64, i64) = db.query_row(
        "SELECT COUNT(*),SUM(CASE WHEN mapping_status='mapped' THEN 1 ELSE 0 END),SUM(CASE WHEN mapping_status<>'mapped' THEN 1 ELSE 0 END),(SELECT COUNT(*) FROM github_work_pool_members WHERE source_id=?1 AND selected=1) FROM github_issue_items WHERE source_id=?1",
        [&source.source_id],
        |row| Ok((row.get(0)?, row.get::<_, Option<i64>>(1)?.unwrap_or(0), row.get::<_, Option<i64>>(2)?.unwrap_or(0), row.get(3)?)),
    )?;
    let active_operation: Option<String> = db
        .query_row(
            "SELECT operation_id FROM github_poll_leases WHERE source_id=?1",
            [&source.source_id],
            |row| row.get(0),
        )
        .optional()?;
    let error = error_json
        .map(|encoded| serde_json::from_str::<Value>(&encoded))
        .transpose()?;
    let coverage: Value = serde_json::from_str(&coverage_json)?;
    Ok(json!({
        "source_id":source.source_id,
        "project_id":source.project_id,
        "host":source.host,
        "owner":source.owner,
        "repo":source.repo,
        "repository_id":source.repository_id,
        "poll_generation":source.poll_generation,
        "next_page":source.next_page,
        "last_poll":{
            "status":status,
            "operation_id":operation,
            "started_at_ms":started,
            "finished_at_ms":finished,
            "error":error,
            "coverage":coverage
        },
        "active_poll_operation_id":active_operation,
        "issue_count":issues,
        "mapped_task_count":mapped,
        "unresolved_mapping_count":unresolved,
        "selected_task_count":selected,
        "deletion_inference":"disabled",
        "comment_coverage":"not_fetched"
    }))
}

fn pool_preview(
    db: &Connection,
    source: &Source,
    request: &protocol::WorkPoolPreviewRequest,
) -> Result<Value> {
    let source_status = source_readback(db, source)?;
    let limit = request.limit.unwrap_or(50) as i64;
    let after = request.after.as_deref();
    let mut statement = db.prepare(
        "SELECT m.task_id,m.issue_id,m.selected,m.selection_order,i.issue_number,i.source_revision,i.mapping_status,i.mapping_error_json,json_extract(i.payload_json,'$.state') FROM github_work_pool_members m JOIN github_issue_items i ON i.source_id=m.source_id AND i.issue_id=m.issue_id WHERE m.source_id=?1 AND (?2 IS NULL OR m.task_id>?2) ORDER BY m.task_id LIMIT ?3",
    )?;
    let rows = statement
        .query_map(params![source.source_id, after, limit + 1], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, bool>(2)?,
                row.get::<_, Option<i64>>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, Option<String>>(7)?,
                row.get::<_, String>(8)?,
            ))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let has_more = rows.len() > limit as usize;
    let rows = rows.into_iter().take(limit as usize).collect::<Vec<_>>();
    let mut items = Vec::with_capacity(rows.len());
    for (
        task_id,
        issue_id,
        selected,
        selection_order,
        number,
        source_revision,
        mapping_status,
        mapping_error,
        issue_state,
    ) in &rows
    {
        let task = tasks::get_task(db, task_id)?;
        items.push(json!({
            "task_id":task_id,
            "task_revision":task["revision"],
            "task_state":task["state"],
            "objective":task["spec"]["objective"],
            "issue_id":issue_id,
            "issue_number":number,
            "issue_state":issue_state,
            "source_revision":source_revision,
            "mapping_status":mapping_status,
            "mapping_error":mapping_error.as_ref().map(|encoded| serde_json::from_str::<Value>(encoded)).transpose()?,
            "selected":selected,
            "selection_order":selection_order
        }));
    }
    let next_cursor = if has_more {
        rows.last().map(|row| row.0.as_str())
    } else {
        None
    };
    Ok(json!({
        "source":source_status,
        "items":items,
        "limit":limit,
        "next_cursor":next_cursor
    }))
}

fn poll_plan(db: &Connection, source_id: &str, operation_id: &str) -> Result<Source> {
    let source = load_source(db, source_id)?;
    let active: Option<String> = db
        .query_row(
            "SELECT operation_id FROM github_poll_leases WHERE source_id=?1",
            [source_id],
            |row| row.get(0),
        )
        .optional()?;
    if active.as_deref() != Some(operation_id) {
        return Err(Error::new(
            "GITHUB_POLL_LEASE_MISSING",
            "the poll Operation no longer owns the source read lease",
        ));
    }
    let operation = operations::get_operation(db, operation_id)?;
    if operation["method"] != "github.source.poll"
        || !matches!(
            operation["state"].as_str(),
            Some("sending" | "outcome_unknown")
        )
    {
        return Err(Error::new(
            "GITHUB_POLL_RECEIPT_INVALID",
            "the source poll Operation no longer matches its retained request",
        ));
    }
    Ok(source)
}

/// Move a queued read Operation to `sending` before leaving the Store for GH
/// I/O. This closes cancellation once a remote read is in flight; after a host
/// restart, `outcome_unknown` can be safely resumed because this adapter only
/// issues GET requests.
fn begin_poll_read(
    db: &mut Connection,
    principal: &Principal,
    source_id: &str,
    operation_id: &str,
) -> Result<()> {
    let now = model::now_ms()?;
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let current = current_principal(&tx, principal.clone())?;
    current.require_operator()?;
    let operation = operations::get_operation(&tx, operation_id)?;
    if operation["method"] != "github.source.poll" || operation["caller_id"] != current.client_id {
        return Err(Error::new(
            "GITHUB_POLL_RECEIPT_INVALID",
            "the source poll Operation does not belong to the authenticated operator",
        ));
    }
    match operation["state"].as_str() {
        Some("queued") => {
            let changed = tx.execute(
                "UPDATE operations SET state='sending',sent_at_ms=?2,updated_at_ms=?2 WHERE operation_id=?1 AND state='queued'",
                params![operation_id, now],
            )?;
            if changed != 1 {
                return Err(Error::conflict(
                    "GitHub poll Operation changed before its bounded read began",
                ));
            }
        }
        Some("outcome_unknown") => {
            let changed = tx.execute(
                "UPDATE operations SET state='sending',sent_at_ms=?2,updated_at_ms=?2 WHERE operation_id=?1 AND state='outcome_unknown'",
                params![operation_id, now],
            )?;
            if changed != 1 {
                return Err(Error::conflict(
                    "GitHub poll Operation changed before its safe read retry began",
                ));
            }
        }
        Some("cancelled") => {
            cleanup_cancelled_poll_in_transaction(&tx, source_id, operation_id, now)?;
            tx.commit()?;
            return Err(Error::new(
                "GITHUB_POLL_CANCELLED",
                "the retained GitHub poll was cancelled before its source read",
            ));
        }
        Some("sending") => {
            return Err(Error::new(
                "GITHUB_POLL_IN_PROGRESS",
                "the retained GitHub poll is already reading; inspect github.source.get",
            ));
        }
        _ => {
            return Err(Error::new(
                "GITHUB_POLL_NOT_RESUMABLE",
                "the retained GitHub poll Operation is not in a resumable read state",
            ));
        }
    }
    let lease: Option<String> = tx
        .query_row(
            "SELECT operation_id FROM github_poll_leases WHERE source_id=?1",
            [source_id],
            |row| row.get(0),
        )
        .optional()?;
    if lease.as_deref() != Some(operation_id) {
        return Err(Error::new(
            "GITHUB_POLL_LEASE_MISSING",
            "the poll Operation no longer owns the source read lease",
        ));
    }
    tx.commit()?;
    Ok(())
}

fn cleanup_cancelled_poll(
    db: &mut Connection,
    principal: &Principal,
    source_id: &str,
    operation_id: &str,
) -> Result<()> {
    let now = model::now_ms()?;
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let current = current_principal(&tx, principal.clone())?;
    current.require_operator()?;
    let operation = operations::get_operation(&tx, operation_id)?;
    if operation["method"] != "github.source.poll"
        || operation["caller_id"] != current.client_id
        || operation["state"] != "cancelled"
    {
        return Err(Error::new(
            "GITHUB_POLL_RECEIPT_INVALID",
            "the cancelled poll Operation does not belong to the authenticated operator",
        ));
    }
    cleanup_cancelled_poll_in_transaction(&tx, source_id, operation_id, now)?;
    tx.commit()?;
    Ok(())
}

fn cleanup_cancelled_poll_in_transaction(
    tx: &Transaction<'_>,
    source_id: &str,
    operation_id: &str,
    now: i64,
) -> Result<()> {
    let active: Option<String> = tx
        .query_row(
            "SELECT operation_id FROM github_poll_leases WHERE source_id=?1",
            [source_id],
            |row| row.get(0),
        )
        .optional()?;
    if active.is_none() {
        return Ok(());
    }
    if active.as_deref() != Some(operation_id) {
        return Err(Error::conflict(
            "a different GitHub poll owns the source read lease",
        ));
    }
    let source = load_source(tx, source_id)?;
    let safe_error = json!({
        "code":"GITHUB_POLL_CANCELLED",
        "message":"the poll was cancelled before its bounded source read began"
    });
    let coverage = json!({
        "kind":"bounded_page_window",
        "starting_page":source.next_page,
        "pages_read":0,
        "reached_end":false,
        "complete_from_page_one":false,
        "returned_issue_count":0,
        "failure_code":"GITHUB_POLL_CANCELLED",
        "failure_message":"the poll was cancelled before its bounded source read began",
        "deletion_inference":"disabled"
    });
    let result = json!({
        "operation_id":operation_id,
        "source_id":source_id,
        "outcome":"cancelled_before_read",
        "error":safe_error,
        "coverage":coverage,
        "deletion_inference":"disabled"
    });
    tx.execute(
        "UPDATE github_sources SET last_poll_status='failed',last_poll_error_json=?2,last_coverage_json=?3,last_poll_operation_id=?4,last_poll_finished_at_ms=?5,updated_at_ms=?5 WHERE source_id=?1",
        params![source_id, model::canonical(&safe_error)?, model::canonical(&coverage)?, operation_id, now],
    )?;
    tx.execute(
        "DELETE FROM github_poll_leases WHERE source_id=?1 AND operation_id=?2",
        params![source_id, operation_id],
    )?;
    tx.execute(
        "INSERT INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES(?1,?2,?2,'github.source.poll.cancelled',?3,?4)",
        params![github_source_stream_id(source_id), operation_id, model::canonical(&result)?, now],
    )?;
    Ok(())
}

fn finish_poll_failure(
    db: &mut Connection,
    principal: &Principal,
    source_id: &str,
    operation_id: &str,
    error: &Error,
) -> Result<()> {
    let now = model::now_ms()?;
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    verify_poll_owner(&tx, principal, source_id, operation_id)?;
    let source = load_source(&tx, source_id)?;
    let coverage = json!({
        "kind":"bounded_page_window",
        "starting_page":source.next_page,
        "pages_read":0,
        "reached_end":false,
        "complete_from_page_one":false,
        "returned_issue_count":0,
        "failure_code":error.code,
        "failure_message":error.message
    });
    let safe_error = json!({"code":error.code,"message":error.message});
    let result = json!({
        "operation_id":operation_id,
        "source_id":source_id,
        "outcome":"read_failed",
        "error":safe_error,
        "coverage":coverage,
        "deletion_inference":"disabled"
    });
    tx.execute(
        "UPDATE github_sources SET last_poll_status='failed',last_poll_error_json=?2,last_coverage_json=?3,last_poll_operation_id=?4,last_poll_finished_at_ms=?5,updated_at_ms=?5 WHERE source_id=?1",
        params![source_id, model::canonical(&safe_error)?, model::canonical(&coverage)?, operation_id, now],
    )?;
    tx.execute(
        "UPDATE operations SET state='rejected',result_json=?2,settled_at_ms=?3,updated_at_ms=?3 WHERE operation_id=?1 AND state IN ('sending','outcome_unknown')",
        params![operation_id, model::canonical(&result)?, now],
    )?;
    tx.execute(
        "DELETE FROM github_poll_leases WHERE source_id=?1 AND operation_id=?2",
        params![source_id, operation_id],
    )?;
    tx.execute(
        "INSERT INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES(?1,?2,?2,'github.source.poll.failed',?3,?4)",
        params![github_source_stream_id(source_id), operation_id, model::canonical(&result)?, now],
    )?;
    tx.commit()?;
    Ok(())
}

fn persist_snapshot(
    db: &mut Connection,
    principal: &Principal,
    source_id: &str,
    operation_id: &str,
    snapshot: &IssueSnapshot,
    config: &Config,
) -> Result<()> {
    let now = model::now_ms()?;
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    verify_poll_owner(&tx, principal, source_id, operation_id)?;
    let source = load_source(&tx, source_id)?;
    if snapshot.repository_id != source.repository_id || snapshot.pages_read == 0 {
        return Err(Error::new(
            "GITHUB_SNAPSHOT_INVALID",
            "bounded GitHub readback did not match the configured source identity or coverage",
        ));
    }
    let generation = source.poll_generation.checked_add(1).ok_or_else(|| {
        Error::new(
            "GITHUB_GENERATION_OVERFLOW",
            "source poll generation exhausted",
        )
    })?;
    let mut facts_added = 0usize;
    let mut tasks_created = 0usize;
    let mut tasks_revised = 0usize;
    let mut mapping_conflicts = 0usize;
    let mut comments_gap_count = 0usize;
    let mut returned_issue_ids = std::collections::BTreeSet::new();

    for issue in &snapshot.issues {
        returned_issue_ids.insert(issue.id);
        if issue.comments > 0 {
            comments_gap_count += 1;
        }
        let fact_digest = work_pool::fact_digest(issue)?;
        let revision = work_pool::source_revision(issue)?;
        let payload = json!(issue);
        let prior = tx
            .query_row(
                "SELECT current_fact_digest,current_event_key,task_id,applied_task_spec_digest FROM github_issue_items WHERE source_id=?1 AND issue_id=?2",
                params![source_id, issue.id],
                |row| {
                    Ok(PriorMapping {
                        current_fact_digest: row.get(0)?,
                        current_event_key: row.get(1)?,
                        task_id: row.get(2)?,
                        applied_task_spec_digest: row.get(3)?,
                    })
                },
            )
            .optional()?;

        let fact_is_new = prior
            .as_ref()
            .is_none_or(|previous| previous.current_fact_digest != fact_digest);
        let event_key = if fact_is_new {
            let previous_event = prior
                .as_ref()
                .map(|previous| previous.current_event_key.as_str());
            let key =
                work_pool::transition_event_key(source_id, issue.id, previous_event, &fact_digest)?;
            tx.execute(
                "INSERT INTO github_issue_facts(source_id,issue_id,event_key,previous_event_key,fact_digest,source_revision,payload_json,operation_id,observed_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",
                params![source_id, issue.id, key, previous_event, fact_digest, revision, model::canonical(&payload)?, operation_id, snapshot.observed_at_ms],
            )?;
            facts_added += 1;
            key
        } else {
            prior
                .as_ref()
                .map(|previous| previous.current_event_key.clone())
                .unwrap_or_default()
        };

        let projected_spec = work_pool::task_spec(&source.host, source.repository_id, issue)?;
        let projected_digest = work_pool::task_spec_digest(&projected_spec)?;
        let mapping = reconcile_task_mapping(
            &tx,
            &TaskMappingContext {
                principal,
                source: &source,
                config,
                now,
            },
            &TaskMappingProjection {
                issue,
                spec: &projected_spec,
                spec_digest: &projected_digest,
                prior: prior.as_ref(),
                event_key: &event_key,
            },
        )?;
        if mapping.created {
            tasks_created += 1;
        }
        if mapping.revised {
            tasks_revised += 1;
        }
        if mapping.status != "mapped" {
            mapping_conflicts += 1;
        }

        let error_json = mapping.error.as_ref().map(model::canonical).transpose()?;
        tx.execute(
            "INSERT INTO github_issue_items(source_id,issue_id,issue_number,current_fact_digest,current_event_key,source_revision,payload_json,task_id,mapping_status,mapping_error_json,applied_task_spec_digest,observed_generation,last_seen_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13) ON CONFLICT(source_id,issue_id) DO UPDATE SET issue_number=excluded.issue_number,current_fact_digest=excluded.current_fact_digest,current_event_key=excluded.current_event_key,source_revision=excluded.source_revision,payload_json=excluded.payload_json,task_id=excluded.task_id,mapping_status=excluded.mapping_status,mapping_error_json=excluded.mapping_error_json,applied_task_spec_digest=excluded.applied_task_spec_digest,observed_generation=excluded.observed_generation,last_seen_at_ms=excluded.last_seen_at_ms",
            params![source_id, issue.id, issue.number, fact_digest, event_key, revision, model::canonical(&payload)?, mapping.task_id, mapping.status, error_json, mapping.applied_spec_digest, generation, snapshot.observed_at_ms],
        )?;
        if mapping.status == "mapped"
            && let Some(task_id) = mapping.task_id
        {
            tx.execute(
                "INSERT INTO github_work_pool_members(source_id,task_id,issue_id,selected,selection_order,discovered_at_ms,updated_at_ms) VALUES(?1,?2,?3,0,NULL,?4,?4) ON CONFLICT(source_id,task_id) DO UPDATE SET issue_id=excluded.issue_id,updated_at_ms=excluded.updated_at_ms",
                params![source_id, task_id, issue.id, snapshot.observed_at_ms],
            )?;
        }
    }

    let next_page = if snapshot.reached_end {
        1u32
    } else {
        snapshot.next_page.ok_or_else(|| {
            Error::new(
                "GITHUB_PAGE_CURSOR_INVALID",
                "incomplete readback has no next page",
            )
        })?
    };
    let complete_from_page_one = snapshot.starting_page == 1 && snapshot.reached_end;
    let (source_unresolved_mappings, source_comments_not_selected): (i64, i64) = tx.query_row(
        "SELECT SUM(CASE WHEN mapping_status<>'mapped' THEN 1 ELSE 0 END),SUM(CASE WHEN COALESCE(json_extract(payload_json,'$.comments'),0)>0 THEN 1 ELSE 0 END) FROM github_issue_items WHERE source_id=?1",
        [source_id],
        |row| Ok((row.get::<_, Option<i64>>(0)?.unwrap_or(0),row.get::<_, Option<i64>>(1)?.unwrap_or(0))),
    )?;
    let status = if complete_from_page_one
        && source_unresolved_mappings == 0
        && source_comments_not_selected == 0
    {
        "complete"
    } else {
        "partial"
    };
    let coverage = json!({
        "kind":"bounded_page_window",
        "starting_page":snapshot.starting_page,
        "pages_read":snapshot.pages_read,
        "next_page":next_page,
        "reached_end":snapshot.reached_end,
        "complete_from_page_one":complete_from_page_one,
        "returned_issue_count":snapshot.issues.len(),
        "distinct_issue_ids":returned_issue_ids.len(),
        "facts_added":facts_added,
        "tasks_created":tasks_created,
        "tasks_revised":tasks_revised,
        "mapping_conflicts":mapping_conflicts,
        "source_unresolved_mappings":source_unresolved_mappings,
        "source_comments_not_selected":source_comments_not_selected,
        "comments_not_selected_issue_count":comments_gap_count,
        "deletion_inference":"disabled"
    });
    let result = json!({
        "operation_id":operation_id,
        "source_id":source_id,
        "outcome":"observed",
        "status":status,
        "poll_generation":generation,
        "coverage":coverage,
        "facts_added":facts_added,
        "tasks_created":tasks_created,
        "tasks_revised":tasks_revised,
        "mapping_conflicts":mapping_conflicts,
        "comments_not_selected_issue_count":comments_gap_count,
        "dispatch_started":false
    });
    tx.execute(
        "UPDATE github_sources SET next_page=?2,poll_generation=?3,last_poll_status=?4,last_poll_error_json=NULL,last_coverage_json=?5,last_poll_operation_id=?6,last_poll_finished_at_ms=?7,updated_at_ms=?7 WHERE source_id=?1",
        params![source_id, next_page, generation, status, model::canonical(&coverage)?, operation_id, now],
    )?;
    tx.execute(
        "UPDATE operations SET state='settled',result_json=?2,settled_at_ms=?3,updated_at_ms=?3 WHERE operation_id=?1 AND state IN ('sending','outcome_unknown')",
        params![operation_id, model::canonical(&result)?, now],
    )?;
    tx.execute(
        "DELETE FROM github_poll_leases WHERE source_id=?1 AND operation_id=?2",
        params![source_id, operation_id],
    )?;
    tx.execute(
        "INSERT INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES(?1,?2,?2,'github.source.poll.observed',?3,?4)",
        params![github_source_stream_id(source_id), operation_id, model::canonical(&result)?, now],
    )?;
    tx.commit()?;
    Ok(())
}

fn reconcile_task_mapping(
    tx: &Transaction<'_>,
    context: &TaskMappingContext<'_>,
    projection: &TaskMappingProjection<'_>,
) -> Result<Mapping> {
    if let Some(previous) = projection.prior
        && let Some(task_id) = previous.task_id.as_deref()
    {
        let task = tasks::get_task(tx, task_id)?;
        let task_spec_digest = work_pool::task_spec_digest(&serde_json::from_value::<TaskSpec>(
            task["spec"].clone(),
        )?)?;
        if task_spec_digest == projection.spec_digest {
            return Ok(Mapping {
                task_id: Some(task_id.to_owned()),
                status: "mapped".to_owned(),
                applied_spec_digest: Some(projection.spec_digest.to_owned()),
                error: None,
                created: false,
                revised: false,
            });
        }
        if previous.applied_task_spec_digest.as_deref() != Some(task_spec_digest.as_str()) {
            return Ok(mapping_conflict(
                task_id,
                "task_spec_conflict",
                "local_task_spec_changed",
            ));
        }
        if task["state"] == "accepted" || task["state"] == "archived" {
            return Ok(mapping_conflict(
                task_id,
                "task_not_revisable",
                "task_terminal_state",
            ));
        }
        let request = json!({
            "client_request_id":task_request_id(projection.event_key),
            "task_id":task_id,
            "expected_revision":task["revision"],
            "spec":projection.spec
        });
        match mutate_in_transaction(
            tx,
            context.principal,
            "task.revise",
            &request,
            context.config,
            context.now,
        )? {
            Ok(_) => {
                let updated = tasks::get_task(tx, task_id)?;
                let updated_spec = serde_json::from_value::<TaskSpec>(updated["spec"].clone())?;
                let updated_digest = work_pool::task_spec_digest(&updated_spec)?;
                if updated_digest != projection.spec_digest {
                    return Ok(mapping_conflict(
                        task_id,
                        "task_spec_conflict",
                        "task_revise_readback_mismatch",
                    ));
                }
                return Ok(Mapping {
                    task_id: Some(task_id.to_owned()),
                    status: "mapped".to_owned(),
                    applied_spec_digest: Some(projection.spec_digest.to_owned()),
                    error: None,
                    created: false,
                    revised: true,
                });
            }
            Err(error) => {
                return Ok(Mapping {
                    task_id: Some(task_id.to_owned()),
                    status: "task_creation_rejected".to_owned(),
                    applied_spec_digest: previous.applied_task_spec_digest.clone(),
                    error: Some(safe_task_mapping_error(&error)),
                    created: false,
                    revised: false,
                });
            }
        }
    }

    let source = context.source;
    let origin_key =
        work_pool::task_origin_key(&source.host, source.repository_id, projection.issue.id);
    let existing: Option<(String, String)> = tx
        .query_row(
            "SELECT task_id,project_id FROM tasks WHERE origin_key=?1",
            [&origin_key],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    if let Some((task_id, project_id)) = existing {
        let task = tasks::get_task(tx, &task_id)?;
        let existing_spec = serde_json::from_value::<TaskSpec>(task["spec"].clone())?;
        if project_id == source.project_id
            && work_pool::task_spec_digest(&existing_spec)? == projection.spec_digest
        {
            return Ok(Mapping {
                task_id: Some(task_id),
                status: "mapped".to_owned(),
                applied_spec_digest: Some(projection.spec_digest.to_owned()),
                error: None,
                created: false,
                revised: false,
            });
        }
        return Ok(mapping_conflict(
            &task_id,
            "task_spec_conflict",
            "origin_key_owned_by_other_spec",
        ));
    }

    let request = json!({
        "client_request_id":task_request_id(projection.event_key),
        "project_id":source.project_id,
        "origin_key":origin_key,
        "spec":projection.spec
    });
    match mutate_in_transaction(
        tx,
        context.principal,
        "task.create",
        &request,
        context.config,
        context.now,
    )? {
        Ok(receipt) => {
            let task_id = model::text(&receipt, "task_id")?.to_owned();
            let task = tasks::get_task(tx, &task_id)?;
            let created_spec = serde_json::from_value::<TaskSpec>(task["spec"].clone())?;
            if work_pool::task_spec_digest(&created_spec)? != projection.spec_digest {
                return Ok(mapping_conflict(
                    &task_id,
                    "task_spec_conflict",
                    "task_create_readback_mismatch",
                ));
            }
            Ok(Mapping {
                task_id: Some(task_id),
                status: "mapped".to_owned(),
                applied_spec_digest: Some(projection.spec_digest.to_owned()),
                error: None,
                created: receipt["created"] == true,
                revised: false,
            })
        }
        Err(error) => Ok(Mapping {
            task_id: None,
            status: "task_creation_rejected".to_owned(),
            applied_spec_digest: None,
            error: Some(safe_task_mapping_error(&error)),
            created: false,
            revised: false,
        }),
    }
}

fn safe_task_mapping_error(error: &Error) -> Value {
    json!({
        "code":error.code,
        "message":"the Task projection was not applied; inspect the retained Task Operation"
    })
}

fn mapping_conflict(task_id: &str, status: &str, reason: &str) -> Mapping {
    Mapping {
        task_id: Some(task_id.to_owned()),
        status: status.to_owned(),
        applied_spec_digest: None,
        error: Some(json!({"code":status,"message":reason})),
        created: false,
        revised: false,
    }
}

fn task_request_id(event_key: &str) -> String {
    format!("github-task-{}", event_key)
}

fn verify_poll_owner(
    tx: &Transaction<'_>,
    principal: &Principal,
    source_id: &str,
    operation_id: &str,
) -> Result<()> {
    principal.require_operator()?;
    let operation = operations::get_operation(tx, operation_id)?;
    if operation["method"] != "github.source.poll"
        || operation["caller_id"] != principal.client_id
        || !matches!(
            operation["state"].as_str(),
            Some("sending" | "outcome_unknown")
        )
    {
        return Err(Error::new(
            "GITHUB_POLL_RECEIPT_INVALID",
            "the queued poll Operation does not belong to the authenticated operator",
        ));
    }
    let active: Option<String> = tx
        .query_row(
            "SELECT operation_id FROM github_poll_leases WHERE source_id=?1",
            [source_id],
            |row| row.get(0),
        )
        .optional()?;
    if active.as_deref() != Some(operation_id) {
        return Err(Error::new(
            "GITHUB_POLL_LEASE_MISSING",
            "the poll Operation does not hold the source read lease",
        ));
    }
    Ok(())
}

fn operation_error(result: &Value) -> Error {
    let error = result.get("error").unwrap_or(result);
    Error::new(
        error["code"].as_str().unwrap_or("GITHUB_POLL_FAILED"),
        error["message"]
            .as_str()
            .unwrap_or("the retained GitHub poll failed; inspect github.source.get"),
    )
}

fn github_source_stream_id(source_id: &str) -> String {
    format!("github:source:{source_id}")
}
