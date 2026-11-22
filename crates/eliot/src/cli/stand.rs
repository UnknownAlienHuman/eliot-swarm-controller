//! Own the directories, expiry, registration, and processes of fresh private stands.

use super::operator::{start_owned_stand, stop_and_drain_stand};
use super::support::{Failure, print_value};
use eliot_core::api::{
    ArtifactKind, ArtifactRegisterRequest, ArtifactView, DEFAULT_STAND_TTL_MS,
    SWARM_STAND_ARTIFACT_PURPOSE,
};
use eliot_daemon::operator::{
    PinnedStandRoot, STAND_MARKER, StandExternalCleanupReport, StandMarker, StandRegistration,
    StandRegistryScan, UnusableStandRegistration, check_directory_chain, check_nested_roots,
    cleanup_stand_external_git, create_stand_root, daemon_alive, daemon_info, data_dirs_match,
    ensure_registry_is_external, inherited_agent_parent_data_dir, list_stand_registrations,
    lock_offline, lock_transition, parse_stand_ttl_ms, read_stand_registration, registry_name,
    remove_offline_stand_root, remove_stand_registration, stand_base, stand_now_ms, stand_registry,
    verify_stand_marker, verify_stand_target, write_stand_registration,
};
use serde_json::{Value, json};
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

const MAX_PROFILE_BYTES: u64 = 1024 * 1024;

pub(crate) async fn up(
    root: Option<PathBuf>,
    profile: Option<PathBuf>,
    binary: Option<PathBuf>,
    ttl: Option<String>,
    json_output: bool,
) -> Result<u8, Failure> {
    let ttl_ms = match ttl.as_deref() {
        Some(value) => parse_stand_ttl_ms(value).map_err(Failure::for_read)?,
        None => DEFAULT_STAND_TTL_MS,
    };
    let registry = stand_registry(true).map_err(Failure::for_read)?;
    let agent_parent = inherited_agent_parent_data_dir();
    let artifact_root = std::env::var_os("ELIOT_ARTIFACT_ROOT")
        .filter(|val| !val.is_empty())
        .map(PathBuf::from);
    let root = match root {
        Some(path) => path,
        None if agent_parent.is_some()
            && std::env::var_os("SWARM_STANDS_ROOT").is_none_or(|val| val.is_empty())
            && artifact_root.is_some() =>
        {
            artifact_root
                .as_ref()
                .expect("checked is_some")
                .join(format!("swarm-stand-{}", ulid::Ulid::new()))
        }
        None => stand_base(true)
            .map_err(Failure::for_read)?
            .join(format!("swarm-stand-{}", ulid::Ulid::new())),
    };
    let root =
        std::path::absolute(root).map_err(|error| io_failure("resolve stand path", error))?;
    let parent = root
        .parent()
        .ok_or_else(|| Failure::new(2, "stand path has no parent directory"))?;
    let name = root
        .file_name()
        .ok_or_else(|| Failure::new(2, "stand path has no directory name"))?;
    check_directory_chain(parent).map_err(Failure::for_read)?;
    let parent = parent
        .canonicalize()
        .map_err(|error| io_failure("resolve stand parent", error))?;
    let allowed_agent_lock_ancestor = check_allowed_agent_parent_ancestor(
        &parent,
        agent_parent.as_deref(),
        artifact_root.as_deref(),
    )?;
    for ancestor in parent.ancestors() {
        if ancestor.join(STAND_MARKER).exists() {
            return Err(Failure::new(
                2,
                "a stand must be outside existing stand directories",
            ));
        }
        if ancestor.join("daemon.lock").exists()
            && allowed_agent_lock_ancestor
                .as_ref()
                .is_none_or(|allowed| !data_dirs_match(ancestor, allowed))
        {
            return Err(Failure::new(
                2,
                "a stand must be outside existing daemon data directories",
            ));
        }
    }
    let root = parent.join(name);
    ensure_registry_is_external(&root, &registry).map_err(Failure::for_read)?;
    let reg_name = registry_name(&root);
    if registry.join(&reg_name).exists() {
        return Err(Failure::new(2, "this path has a prior stand registration"));
    }
    let binary = match binary {
        Some(binary) => binary,
        None => std::env::current_exe().map_err(|error| io_failure("locate Swarm", error))?,
    }
    .canonicalize()
    .map_err(|error| io_failure("resolve stand binary", error))?;
    if !binary.is_file() {
        return Err(Failure::new(2, "stand binary must be a file"));
    }
    let profile = profile.as_deref().map(read_profile).transpose()?;

    create_stand_root(&root).map_err(Failure::for_read)?;
    let root = root
        .canonicalize()
        .map_err(|error| io_failure("resolve created stand", error))?;
    // Hold the directory just created and every ancestor by handle until the
    // daemon's owner token is bound: none of them can be renamed or swapped
    // for a junction meanwhile. Every later owner operation requires this
    // exact directory identity.
    let pinned_root = PinnedStandRoot::open(&root).map_err(Failure::for_read)?;
    let root_identity = pinned_root.identity().to_owned();
    let workspace = root.join("repo");
    if let Err(failure) = prepare(&root, &workspace, profile).await {
        drop(pinned_root);
        super::cleanup::remove_dir_all(&root).map_err(|error| {
            io_failure(
                &format!(
                    "{}; remove incomplete stand {}",
                    failure.message,
                    root.display()
                ),
                error,
            )
        })?;
        return Err(failure);
    }
    let _transition = lock_transition(&root).map_err(Failure::for_read)?;
    let created_at = stand_now_ms();
    let expires_at = created_at
        .checked_add(ttl_ms)
        .ok_or_else(|| Failure::new(2, "stand expiry overflows epoch timestamp"))?;
    let mut registration = StandRegistration {
        marker: StandMarker {
            format: 2,
            root: root.clone(),
            token: format!("{}{}", ulid::Ulid::new(), ulid::Ulid::new()),
            created_at: Some(created_at),
            expires_at: Some(expires_at),
        },
        removing_pid: None,
        artifact_id: None,
        // The issuing daemon is persisted at creation; later checks never
        // read it from inherited environment.
        parent_data_dir: agent_parent.clone(),
        owner_token_sha256: None,
        root_identity: Some(root_identity),
        pending_write_guard: None,
        settled_external_worktrees: Vec::new(),
        settled_external_branches: Vec::new(),
    };
    let mut marker = File::create_new(root.join(STAND_MARKER))
        .map_err(|error| io_failure("create stand marker", error))?;
    let bytes = serde_json::to_vec(&registration.marker)
        .map_err(|_| Failure::new(1, "encode stand marker"))?;
    marker
        .write_all(&bytes)
        .and_then(|()| marker.sync_all())
        .map_err(|error| io_failure("write stand marker", error))?;
    // Close the marker before the bind reads it through the pinned root.
    drop(marker);
    write_stand_registration(&registry, &reg_name, &registration).map_err(Failure::for_read)?;

    if let Some(parent_dir) = agent_parent.as_ref()
        && artifact_root
            .as_ref()
            .and_then(|path| path.canonicalize().ok())
            .is_some_and(|art_root| root.starts_with(&art_root))
    {
        let request = ArtifactRegisterRequest {
            id: ulid::Ulid::new().to_string(),
            path: root.to_string_lossy().into_owned(),
            kind: ArtifactKind::DataDir,
            purpose: SWARM_STAND_ARTIFACT_PURPOSE.to_owned(),
            expires_at: Some(expires_at),
        };
        let registered = eliot_daemon::client_call_typed::<_, ArtifactView>(
            parent_dir,
            "artifact.register",
            &request,
        )
        .await;
        match registered {
            Ok(view) => {
                registration.artifact_id = Some(view.id);
                write_stand_registration(&registry, &reg_name, &registration)
                    .map_err(Failure::for_read)?;
            }
            Err(error) => {
                drop(_transition);
                drop(pinned_root);
                let _ = remove_stand_registration(&registry, &reg_name);
                let _ = super::cleanup::remove_dir_all(&root);
                return Err(Failure::for_mutation(error));
            }
        }
    }

    // Pass the display path (without Windows \\?\ prefix) so stand worktrees can freeze cleanly.
    let daemon_root = PathBuf::from(display(&root));
    let pid = start_owned_stand(&daemon_root, &binary)
        .await
        .map_err(|mut failure| {
            failure.message = format!("{}; stand retained at {}", failure.message, display(&root));
            failure
        })?;
    // The owner token is bound; release the creation pin.
    drop(pinned_root);
    let data_dir = display(&root);
    let workspace = display(&workspace);
    if json_output {
        print_value(
            &json!({
                "state": "ready",
                "data_dir": data_dir,
                "workspace": workspace,
                "pid": pid,
                "created_at": created_at,
                "expires_at": expires_at,
                "artifact_id": registration.artifact_id,
            }),
            true,
        );
    } else {
        println!("Stand ready at {data_dir} (daemon PID {pid})");
        println!("Scratch Git repository at {workspace}");
        println!("Expires at {expires_at} ms (TTL {} ms)", ttl_ms);
        if let Some(artifact_id) = registration.artifact_id.as_deref() {
            println!("Registered with parent daemon as artifact {artifact_id}");
        }
        println!("swarm --data-dir \"{data_dir}\" status");
        println!("swarm stand down \"{data_dir}\"");
    }
    Ok(0)
}

fn check_allowed_agent_parent_ancestor(
    canonical_stand_parent: &Path,
    agent_parent: Option<&Path>,
    artifact_root: Option<&Path>,
) -> Result<Option<PathBuf>, Failure> {
    let (Some(agent_parent), Some(artifact_root)) = (agent_parent, artifact_root) else {
        return Ok(None);
    };
    let Ok(canonical_agent_parent) = agent_parent.canonicalize() else {
        return Ok(None);
    };
    let Ok(canonical_artifact_root) = artifact_root.canonicalize() else {
        return Ok(None);
    };
    if !canonical_stand_parent.starts_with(&canonical_artifact_root) {
        return Ok(None);
    }
    // No nested daemon.lock is allowed anywhere inside ELIOT_ARTIFACT_ROOT itself.
    if canonical_stand_parent
        .ancestors()
        .take_while(|ancestor| *ancestor != canonical_artifact_root)
        .any(|ancestor| ancestor.join("daemon.lock").exists())
        || canonical_artifact_root.join("daemon.lock").exists()
    {
        return Err(Failure::new(
            2,
            "a stand inside ELIOT_ARTIFACT_ROOT must not sit inside another daemon lock",
        ));
    }
    Ok(Some(canonical_agent_parent))
}

struct DownOutcome {
    state: &'static str,
    data_dir: String,
    daemon_pid: u32,
    external_worktrees: Vec<eliot_daemon::operator::StandWorktreeOutcome>,
    external_branches: Vec<eliot_daemon::operator::StandBranchOutcome>,
    unusable_records: Vec<String>,
}

pub(crate) async fn down(path: PathBuf, json_output: bool) -> Result<u8, Failure> {
    let outcome = tokio::spawn(async move { Box::pin(down_inner(&path)).await })
        .await
        .map_err(|error| Failure::new(1, format!("stand teardown task failed: {error}")))??;
    let payload = json!({
        "state": outcome.state,
        "data_dir": outcome.data_dir,
        "daemon_pid": outcome.daemon_pid,
        "external_worktrees": outcome.external_worktrees,
        "external_branches": outcome.external_branches,
        "unusable_records": outcome.unusable_records,
    });
    if json_output {
        print_value(&payload, true);
    } else if outcome.state == "removed" {
        println!(
            "Removed {} after daemon PID {} and its owned processes exited",
            outcome.data_dir, outcome.daemon_pid
        );
        print_external_summary(&outcome.external_worktrees, &outcome.external_branches);
    } else {
        eprintln!(
            "Retained {} (daemon PID {} stopped) because one or more external Git resources or creation records were kept or unknown:",
            outcome.data_dir, outcome.daemon_pid
        );
        print_external_summary(&outcome.external_worktrees, &outcome.external_branches);
        for message in &outcome.unusable_records {
            println!("  {message}");
        }
        eprintln!(
            "Clean, merge or repair kept items, then retry: swarm stand down \"{}\"",
            outcome.data_dir
        );
    }
    Ok(if outcome.state == "removed" { 0 } else { 4 })
}

async fn down_inner(path: &Path) -> Result<DownOutcome, Failure> {
    let path =
        std::path::absolute(path).map_err(|error| io_failure("resolve stand path", error))?;
    let registry = stand_registry(false).map_err(Failure::for_read)?;
    let parent = path
        .parent()
        .ok_or_else(|| Failure::new(2, "stand path has no parent"))?;
    check_directory_chain(parent).map_err(Failure::for_read)?;
    let name = path
        .file_name()
        .ok_or_else(|| Failure::new(2, "stand path has no name"))?;
    let root = parent
        .canonicalize()
        .map_err(|error| io_failure("resolve stand parent", error))?
        .join(name);
    let reg_name = registry_name(&root);
    ensure_registry_is_external(&root, &registry).map_err(Failure::for_read)?;
    let mut record =
        read_stand_registration(&registry, &reg_name, &root).map_err(Failure::for_read)?;
    match fs::symlink_metadata(&root) {
        Err(error)
            if error.kind() == std::io::ErrorKind::NotFound && record.removing_pid.is_some() =>
        {
            remove_stand_registration(&registry, &reg_name).map_err(Failure::for_read)?;
            return Ok(DownOutcome {
                state: "removed",
                data_dir: display(&root),
                daemon_pid: record.removing_pid.unwrap_or_default(),
                external_worktrees: Vec::new(),
                external_branches: Vec::new(),
                unusable_records: Vec::new(),
            });
        }
        Err(error) => return Err(io_failure("inspect stand root", error)),
        Ok(_) => {}
    }
    check_directory_chain(&root).map_err(Failure::for_read)?;
    verify_stand_marker(&root, &record).map_err(Failure::for_read)?;
    // Before any action (desktop stop, drain, removal): never a protected
    // root, and only the stand whose owner token `stand up` bound.
    verify_stand_target(&root, &record).map_err(Failure::for_read)?;
    let _transition = lock_transition(&root).map_err(Failure::for_read)?;
    record = read_stand_registration(&registry, &reg_name, &root).map_err(Failure::for_read)?;
    verify_stand_marker(&root, &record).map_err(Failure::for_read)?;
    check_nested_roots(&root).map_err(Failure::for_read)?;
    let pid = match record.removing_pid {
        Some(pid) => pid,
        None => prove_stand_exit(&root).await?,
    };
    let offline = lock_offline(&root).map_err(Failure::for_read)?;
    if let Some(info) = daemon_info(&root).map_err(Failure::for_read)?
        && ((record.removing_pid.is_none() && info.pid != pid)
            || daemon_alive(&info).map_err(Failure::for_read)?)
    {
        return Err(Failure::new(
            4,
            "stand daemon identity changed; files retained",
        ));
    }
    let report: StandExternalCleanupReport = Box::pin(cleanup_stand_external_git(
        &root,
        &root,
        &registry,
        &reg_name,
        &mut record,
    ))
    .await
    .map_err(Failure::for_mutation)?;
    if report.has_retained_or_unknown() {
        return Ok(DownOutcome {
            state: "retained",
            data_dir: display(&root),
            daemon_pid: pid,
            external_worktrees: report.worktrees,
            external_branches: report.branches,
            unusable_records: report.unusable_records,
        });
    }
    record.removing_pid = Some(pid);
    write_stand_registration(&registry, &reg_name, &record).map_err(Failure::for_read)?;
    remove_offline_stand_root(&root, offline).map_err(Failure::for_read)?;
    remove_stand_registration(&registry, &reg_name).map_err(Failure::for_read)?;
    Ok(DownOutcome {
        state: "removed",
        data_dir: display(&root),
        daemon_pid: pid,
        external_worktrees: report.worktrees,
        external_branches: report.branches,
        unusable_records: report.unusable_records,
    })
}

fn scan_registry() -> Result<StandRegistryScan, Failure> {
    let mut scan = match stand_registry(false) {
        Ok(registry) => list_stand_registrations(&registry).map_err(Failure::for_read)?,
        Err(error) if error.code == "not_found" => StandRegistryScan::default(),
        Err(error) => return Err(Failure::for_read(error)),
    };
    scan.former = eliot_daemon::operator::former_stand_registrations();
    Ok(scan)
}

fn print_unusable(unusable: &[UnusableStandRegistration], former: &[UnusableStandRegistration]) {
    for item in unusable {
        println!("{} | unusable: {}", item.registry_path, item.detail);
    }
    for item in former {
        println!("{} | {}", item.registry_path, item.detail);
    }
}

pub(crate) async fn list(json_output: bool) -> Result<u8, Failure> {
    let StandRegistryScan {
        entries,
        unusable,
        former,
    } = scan_registry()?;
    let now = stand_now_ms();
    let mut stands = Vec::with_capacity(entries.len());
    for entry in entries {
        let root = &entry.registration.marker.root;
        let exists = fs::symlink_metadata(root).is_ok();
        let (alive, pid) = match daemon_info(root) {
            Ok(Some(info)) => (daemon_alive(&info).unwrap_or(false), Some(info.pid)),
            _ => (false, None),
        };
        let expired = entry
            .registration
            .marker
            .expires_at
            .is_some_and(|exp| now >= exp);
        stands.push(json!({
            "data_dir": display(root),
            "exists": exists,
            "alive": alive,
            "pid": pid,
            "created_at": entry.registration.marker.created_at,
            "expires_at": entry.registration.marker.expires_at,
            "expired": expired,
            "artifact_id": entry.registration.artifact_id,
            "removing_pid": entry.registration.removing_pid,
        }));
    }
    let payload = json!({ "stands": stands, "unusable": unusable, "former": former });
    if json_output {
        print_value(&payload, true);
    } else if stands.is_empty() && unusable.is_empty() && former.is_empty() {
        println!("No registered stands.");
    } else {
        for item in &stands {
            println!(
                "{} | alive={} | expired={} | expires_at={}",
                item["data_dir"].as_str().unwrap_or(""),
                item["alive"].as_bool().unwrap_or(false),
                item["expired"].as_bool().unwrap_or(false),
                item["expires_at"]
                    .as_i64()
                    .map_or_else(|| "none".to_owned(), |v| v.to_string()),
            );
        }
        print_unusable(&unusable, &former);
    }
    Ok(0)
}

pub(crate) async fn prune(json_output: bool) -> Result<u8, Failure> {
    let StandRegistryScan {
        entries,
        unusable,
        former,
    } = scan_registry()?;
    let now = stand_now_ms();
    let mut results: Vec<Value> = Vec::with_capacity(entries.len());
    // An unusable registration cannot prove its expiry or root, so it is
    // reported and never torn down.
    let mut any_blocked = !unusable.is_empty();
    for entry in entries {
        let root = entry.registration.marker.root.clone();
        let data_dir = display(&root);
        let expired = entry
            .registration
            .marker
            .expires_at
            .is_some_and(|exp| now >= exp);
        if !expired {
            results.push(json!({
                "data_dir": data_dir,
                "outcome": "skipped",
                "reason": "not_expired",
                "expires_at": entry.registration.marker.expires_at,
            }));
            continue;
        }
        let root_for_down = root.clone();
        let down_result = tokio::spawn(async move { Box::pin(down_inner(&root_for_down)).await })
            .await
            .map_err(|error| Failure::new(1, format!("stand prune task failed: {error}")))?;
        match down_result {
            Ok(outcome) if outcome.state == "removed" => {
                results.push(json!({
                    "data_dir": outcome.data_dir,
                    "outcome": "removed",
                    "daemon_pid": outcome.daemon_pid,
                    "external_worktrees": outcome.external_worktrees,
                    "external_branches": outcome.external_branches,
                }));
            }
            Ok(outcome) => {
                any_blocked = true;
                results.push(json!({
                    "data_dir": outcome.data_dir,
                    "outcome": "retained",
                    "daemon_pid": outcome.daemon_pid,
                    "external_worktrees": outcome.external_worktrees,
                    "external_branches": outcome.external_branches,
                    "unusable_records": outcome.unusable_records,
                }));
            }
            Err(error) => {
                any_blocked = true;
                results.push(json!({
                    "data_dir": data_dir,
                    "outcome": "error",
                    "reason": error.code.unwrap_or_else(|| "error".to_owned()),
                    "message": error.message,
                }));
            }
        }
    }
    let payload = json!({ "stands": results, "unusable": unusable, "former": former });
    if json_output {
        print_value(&payload, true);
    } else if results.is_empty() && unusable.is_empty() && former.is_empty() {
        println!("No registered stands to prune.");
    } else {
        for item in &results {
            let why = item
                .get("message")
                .and_then(Value::as_str)
                .map(|message| format!(": {message}"))
                .or_else(|| {
                    item.get("reason")
                        .and_then(Value::as_str)
                        .map(|reason| format!(" ({reason})"))
                })
                .unwrap_or_default();
            println!(
                "{} | outcome={}{why}",
                item["data_dir"].as_str().unwrap_or(""),
                item["outcome"].as_str().unwrap_or(""),
            );
            for key in ["external_worktrees", "external_branches"] {
                for detail in item[key]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|outcome| outcome["detail"].as_str())
                {
                    println!("  {detail}");
                }
            }
            for message in item["unusable_records"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
            {
                println!("  {message}");
            }
        }
        print_unusable(&unusable, &former);
    }
    Ok(if any_blocked { 4 } else { 0 })
}

fn print_external_summary(
    worktrees: &[eliot_daemon::operator::StandWorktreeOutcome],
    branches: &[eliot_daemon::operator::StandBranchOutcome],
) {
    for wt in worktrees {
        println!(
            "  external worktree {} (branch {}): {}{}",
            wt.worktree_path,
            wt.branch,
            wt.outcome,
            outcome_note(wt.reason.as_deref(), wt.detail.as_deref())
        );
    }
    for branch in branches {
        println!(
            "  external branch {} in {}: {}{}",
            branch.branch,
            branch.repo_common_dir,
            branch.outcome,
            outcome_note(branch.reason.as_deref(), branch.detail.as_deref())
        );
    }
}

fn outcome_note(reason: Option<&str>, detail: Option<&str>) -> String {
    let reason = reason.map_or_else(String::new, |reason| format!(" ({reason})"));
    let detail = detail.map_or_else(String::new, |detail| format!(": {detail}"));
    format!("{reason}{detail}")
}

async fn prove_stand_exit(root: &Path) -> Result<u32, Failure> {
    let daemon = daemon_info(root)
        .map_err(Failure::for_read)?
        .ok_or_else(|| unproven_exit(root))?;
    #[cfg(windows)]
    eliot_daemon::operator::stop_desktop(root).map_err(Failure::for_read)?;
    if daemon_alive(&daemon).map_err(Failure::for_read)? {
        stop_and_drain_stand(root).await?;
    }
    let current = daemon_info(root).map_err(Failure::for_read)?;
    if current.as_ref() != Some(&daemon)
        || daemon_alive(&daemon).map_err(Failure::for_read)?
        || eliot_daemon::last_daemon_phase(root).as_deref() != Some("daemon.stopped")
    {
        return Err(unproven_exit(root));
    }
    Ok(daemon.pid)
}

fn unproven_exit(root: &Path) -> Failure {
    Failure::new(
        4,
        format!(
            "stand lacks a clean exit record; files retained. Start this registered stand with swarm --data-dir \"{}\" daemon to reconcile custody, then retry stand down",
            display(root)
        ),
    )
}

async fn prepare(root: &Path, workspace: &Path, profile: Option<Vec<u8>>) -> Result<(), Failure> {
    fs::create_dir(workspace).map_err(|error| io_failure("create scratch repository", error))?;
    git(workspace, &["init", "--quiet", "--template="]).await?;
    git(
        workspace,
        &[
            "-c",
            "user.name=Swarm Stand",
            "-c",
            "user.email=stand@eliot.invalid",
            "-c",
            "commit.gpgSign=false",
            "commit",
            "--quiet",
            "--allow-empty",
            "-m",
            "Initialize scratch repository",
        ],
    )
    .await?;
    if let Some(profile) = profile {
        eliot_daemon::operator::write_stand_config(root, &profile).map_err(Failure::for_read)?;
    }
    Ok(())
}

async fn git(workspace: &Path, args: &[&str]) -> Result<(), Failure> {
    #[expect(
        clippy::disallowed_methods,
        reason = "This statement constructs a process owned by this module."
    )]
    #[expect(
        clippy::disallowed_types,
        reason = "This helper invokes Git while assembling the private QA stand."
    )]
    let mut command = tokio::process::Command::new("git");
    for (name, _) in std::env::vars_os() {
        if name
            .to_string_lossy()
            .to_ascii_uppercase()
            .starts_with("GIT_")
        {
            command.env_remove(name);
        }
    }
    #[cfg(windows)]
    const NULL_DEVICE: &str = "NUL";
    #[cfg(not(windows))]
    const NULL_DEVICE: &str = "/dev/null";
    command
        .current_dir(eliot_core::display_path(&workspace.to_string_lossy()))
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", NULL_DEVICE)
        .args(args)
        .stdin(Stdio::null())
        .kill_on_drop(true);
    let output = tokio::time::timeout(Duration::from_secs(10), command.output())
        .await
        .map_err(|_| Failure::new(5, "scratch Git setup timed out"))?
        .map_err(|error| io_failure("run scratch Git setup", error))?;
    if !output.status.success() {
        return Err(Failure::new(
            1,
            format!(
                "scratch Git setup failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            ),
        ));
    }
    Ok(())
}

fn read_profile(path: &Path) -> Result<Vec<u8>, Failure> {
    let file = File::open(path)
        .map_err(|error| io_failure("open private profile configuration", error))?;
    if !file
        .metadata()
        .map_err(|error| io_failure("inspect private profile configuration", error))?
        .is_file()
    {
        return Err(Failure::new(
            2,
            "stand profile configuration must be a regular file",
        ));
    }
    let mut profile = Vec::new();
    file.take(MAX_PROFILE_BYTES + 1)
        .read_to_end(&mut profile)
        .map_err(|error| io_failure("read private profile configuration", error))?;
    if profile.len() as u64 > MAX_PROFILE_BYTES {
        return Err(Failure::new(2, "stand profile configuration exceeds 1 MiB"));
    }
    eliot_daemon::operator::validate_stand_profiles(&profile).map_err(Failure::for_read)?;
    Ok(profile)
}

fn display(path: &Path) -> String {
    eliot_core::display_path(&path.to_string_lossy())
}

fn io_failure(context: &str, error: std::io::Error) -> Failure {
    Failure::new(1, format!("{context}: {error}"))
}
