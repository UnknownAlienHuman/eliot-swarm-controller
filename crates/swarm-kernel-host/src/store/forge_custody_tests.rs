//! Windows regressions for the production Forge worker custody path.
//! Requires the same-source swarm-forge-worker.exe beside the lib test executable.
//! Root wires this file as a child test module of store::forge.

#![cfg(windows)]

use super::*;

const FIXTURE_BOUND: Duration = Duration::from_secs(5);

struct DeniedWorkerFixture {
    directory: PathBuf,
    job: NativeWorkerJob,
    lane: Arc<ForgeTargetLane>,
    custody: ForgeGitCustody,
}

impl DeniedWorkerFixture {
    fn new() -> Self {
        let worker = forge_worker_executable().expect(
            "fixture prerequisite: stage the same-source swarm-forge-worker.exe beside the swarm_kernel_host lib test executable",
        );
        let directory =
            std::env::temp_dir().join(format!("swarm-forge-custody-{}", model::new_id()));
        ensure_private_forge_directory(&directory).unwrap();
        let directory = directory.canonicalize().unwrap();
        let intent = PublicationIntent {
            operation_id: model::new_id(),
            project_id: "forge-custody-fixture".into(),
            canonical_repository: "github.com/fixture/forge-custody".into(),
            attempt_id: model::new_id(),
            task_revision: 1,
            admitted_gm_epoch: 0,
            submission_ref: "submission-fixture".into(),
            accepted_operation_id: model::new_id(),
            candidate_ref: "candidate-fixture".into(),
            candidate_sha256: "a".repeat(64),
            commit: "b".repeat(40),
            tree: "c".repeat(40),
            remote_name: "origin".into(),
            target_ref: format!("refs/heads/custody-{}", model::new_id()),
            expected_old_ref: Some("d".repeat(40)),
            expected_create: false,
            force: false,
            policy_revision: "custody-fixture-v1".into(),
        };
        intent.validate().unwrap();
        let project = ForgeProject {
            canonical_repository: intent.canonical_repository.clone(),
            repository_path: directory.clone(),
            remote_name: intent.remote_name.clone(),
            policy_revision: intent.policy_revision.clone(),
            target_refs: vec![intent.target_ref.clone()],
        };
        let mut config = ForgeConfig {
            enabled: true,
            // Denial must never invoke this pin. Reusing the staged worker
            // avoids a Git/network prerequisite and cannot perform a push.
            git_executable: worker,
            timeout_seconds: 1,
            max_output_bytes: 1_024,
            ..Default::default()
        };
        config
            .projects
            .insert(intent.project_id.clone(), project.clone());
        let lane = target_lane(&ForgeTargetKey::from_intent(&intent).unwrap());
        let work = ForgeWork {
            intent,
            lane: Arc::clone(&lane),
            project,
            candidate: None,
            mode: WorkMode::ReadbackOnly,
            execution: Some(ForgeExecutionPin::capture(&config).unwrap()),
            transfer_continuation: None,
        };
        let job = create_forge_worker_job(&directory, &work, &config, None).unwrap();
        Self {
            directory,
            job,
            lane,
            custody: ForgeGitCustody::default(),
        }
    }

    async fn spawn(&self) -> NativeWorkerRun {
        let permit = Arc::clone(&self.lane.process).try_acquire_owned().unwrap();
        match spawn_native_worker(
            &self.custody,
            self.job.clone(),
            permit,
            Some(Arc::clone(&self.lane)),
        )
        .await
        {
            Ok(run) => run,
            Err(error) => {
                let cleanup = self.abort_owned_child().await;
                panic!("production Forge startup failed: {error:?}; {cleanup}");
            }
        }
    }

    async fn abort_owned_child(&self) -> String {
        let entry = self
            .custody
            .state
            .lock()
            .unwrap()
            .native_workers
            .first()
            .cloned();
        let Some(entry) = entry else {
            return "no registered child; fixture files retained".into();
        };
        // Failure cleanup uses only the retained direct Child. It never
        // manufactures family departure or authorizes the worker.
        let kill = entry.lock().unwrap().child.start_kill();
        let exit = tokio::time::timeout(FIXTURE_BOUND, async {
            loop {
                match entry.lock().unwrap().observe_exit() {
                    Ok(Some(status)) => return Ok(status),
                    Err(error) => return Err(error),
                    Ok(None) => {}
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        let pending = self.custody.poll();
        format!(
            "exact-child cleanup: kill={kill:?}, exit={exit:?}, custody_pending={pending}; fixture files retained"
        )
    }

    async fn finish_denied(&self, run: NativeWorkerRun) -> Value {
        let entry = Arc::clone(&run.custody);
        // Also allow cleanup after a failed readiness assertion, but callers
        // retain and assert the readiness facts captured before this fallback.
        let query = entry.lock().unwrap().retain_job_query(&run.owner_record);
        if let Err(error) = query {
            drop(run);
            let cleanup = self.abort_owned_child().await;
            panic!("query unavailable before fixture denial: {error:?}; {cleanup}");
        }
        if let Err(error) = write_worker_authorization(&self.job, false) {
            drop(run);
            let cleanup = self.abort_owned_child().await;
            panic!("fixture denial failed: {error:?}; {cleanup}");
        }
        let waited = tokio::time::timeout(FIXTURE_BOUND, wait_forge_worker(run, false)).await;
        let result = match waited {
            Ok(Ok(result)) => result,
            other => {
                let cleanup = self.abort_owned_child().await;
                panic!("production denied-worker wait failed: {other:?}; {cleanup}");
            }
        };
        let confirmed = {
            let entry = entry.lock().unwrap();
            entry.status.is_some_and(|status| status.success())
                && entry.family_departed
                && !entry.is_pending()
                && !entry.active
                && matches!(&entry.job_query, NativeWorkerJobQuery::Retained(_))
        };
        drop(entry);
        let pending = self.custody.poll();
        let permits = self.lane.process.available_permits();
        if confirmed && !pending && permits == 1 {
            fs::remove_dir_all(&self.directory).unwrap();
        }
        assert!(
            confirmed,
            "the exact worker family was not confirmed departed"
        );
        assert!(
            !pending,
            "completed production custody must retire its entry"
        );
        assert_eq!(
            permits, 1,
            "release capacity only after confirmed custody retirement"
        );
        result
    }
}

#[tokio::test]
async fn forge_denied_worker_uses_retained_job_query_through_production_custody() {
    let fixture = DeniedWorkerFixture::new();
    let run = fixture.spawn().await;
    let (pid, query_retained, family_present, child_running) = {
        let mut entry = run.custody.lock().unwrap();
        (
            entry.pid,
            matches!(&entry.job_query, NativeWorkerJobQuery::Retained(_)),
            matches!(entry.family_empty(), Ok(false)),
            matches!(entry.observe_exit(), Ok(None)),
        )
    };
    let valid_owner = validate_worker_owner(&fixture.job, pid, &run.owner_record).is_ok();
    let held = fixture.custody.poll() && fixture.lane.process.available_permits() == 0;
    let no_effect = matches!(fixture.job.authorization_path.try_exists(), Ok(false))
        && matches!(fixture.job.result_path.try_exists(), Ok(false));
    let result = fixture.finish_denied(run).await;

    assert!(valid_owner && query_retained && family_present && child_running);
    assert!(
        held && no_effect,
        "ready worker must remain behind authorization with its permit held"
    );
    assert_eq!(result["outcome"], "not_authorized");
    assert_eq!(result["reason"], "authorization_denied");
    assert_eq!(result["authorized"], false);
    assert_eq!(result["process_tree_empty"], true);
    assert_eq!(result["process_tree_unconfirmed"], false);
    assert!(result["remote_ref"].is_null() && result["push_exit_code"].is_null());
}

#[tokio::test]
async fn forge_unavailable_query_holds_owner_readiness_without_authorizing() {
    let fixture = DeniedWorkerFixture::new();
    let mut run = fixture.spawn().await;
    let entry = Arc::clone(&run.custody);
    {
        let mut entry = entry.lock().unwrap();
        // Force a real query-acquisition error using a parent-local token
        // mismatch; the live worker's plan and validated receipt stay intact.
        entry.job_query = NativeWorkerJobQuery::Pending;
        entry.job.owner_token = model::new_id();
        entry.owner_record = None;
    }
    let held = tokio::time::timeout(FIXTURE_BOUND, async {
        let ready = wait_worker_owner(&run);
        tokio::pin!(ready);
        loop {
            tokio::select! {
                biased;
                returned = &mut ready => {
                    return Err(format!("readiness returned without its query: {returned:?}"));
                }
                () = tokio::time::sleep(Duration::from_millis(10)) => {
                    let entry = entry.lock().unwrap();
                    // Both writes occur under the production gate's lock, so
                    // this observes a validated owner plus a failed query attempt.
                    if entry.owner_record.is_some()
                        && matches!(&entry.job_query, NativeWorkerJobQuery::Pending)
                    {
                        return Ok(());
                    }
                }
            }
        }
    })
    .await;
    let unknown = {
        let entry = entry.lock().unwrap();
        matches!(entry.family_empty(), Err(error) if error.code == "FORGE_GIT_TREE_TERMINATION")
            && !entry.family_departed
            && entry.is_pending()
    };
    let no_effect = matches!(fixture.job.authorization_path.try_exists(), Ok(false))
        && matches!(fixture.job.result_path.try_exists(), Ok(false))
        && fixture.lane.process.available_permits() == 0;
    entry.lock().unwrap().job.owner_token = fixture.job.owner_token.clone();
    let restored = tokio::time::timeout(FIXTURE_BOUND, wait_worker_owner(&run)).await;
    let restored_ready = match restored {
        Ok(Ok(owner)) => {
            run.owner_record = owner;
            true
        }
        _ => false,
    };
    let restored_query = matches!(
        &entry.lock().unwrap().job_query,
        NativeWorkerJobQuery::Retained(_)
    );
    drop(entry);
    let result = fixture.finish_denied(run).await;

    assert!(
        matches!(&held, Ok(Ok(()))),
        "failed query must hold readiness: {held:?}"
    );
    assert!(
        unknown && no_effect,
        "unavailable accounting must remain uncertain without authorizing an effect"
    );
    assert!(
        restored_ready && restored_query,
        "validated owner must permit a later query retry"
    );
    assert_eq!(result["outcome"], "not_authorized");
    assert_eq!(result["reason"], "authorization_denied");
}
