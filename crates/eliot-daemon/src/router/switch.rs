//! One durable switch worker composes normal launch and H2 transfer.
use super::*;
use eliot_core::api::{SWITCH_BLANK_SCOPE_ARG, SWITCH_PROJECT_REVISION_ARG, SwitchRequest};

/// Whether a switch keeps its source checkout: it requests neither a
/// workspace override nor a new branch. Reuse is selected before any
/// preparation so the automatic worktree policy never mints (or fails to
/// mint) a fresh worktree the replacement would drop anyway.
fn reuse_source_checkout(launch: &eliot_core::api::SwitchLaunch) -> bool {
    launch.workspace.is_none() && launch.worktree.is_none()
}

/// Whether a switch with no workspace override reuses the retained
/// checkout: a freshly prepared worktree would leave the source's
/// uncommitted edits behind in the old directory. Only an automatically
/// minted worktree is dropped; an explicitly requested branch stays
/// prepared from the retained checkout (its `generated.branch` is set).
fn reuse_retained_checkout(
    requested_workspace: Option<&str>,
    prepared: &Option<WorkspacePreparationInput>,
) -> bool {
    requested_workspace.is_none()
        && prepared.as_ref().is_some_and(|workspace| {
            workspace
                .generated
                .as_ref()
                .is_some_and(|naming| naming.branch.is_none())
        })
}

impl Router {
    pub(super) async fn swarm_switch(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        require_owner(principal)?;
        let request: SwitchRequest = eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let policy = self.secret_policy_snapshot();
        let change = {
            let mut store = self.inner.store.lock().await;
            if let Some(value) = store.replay_switch(&principal_name(principal), &request)? {
                return serde_json::to_value(value)
                    .map_err(|_| Error::new("internal", "cannot encode switch replay"));
            }
            if self.inner.mode.load(Ordering::SeqCst) != MODE_OPEN {
                return Err(Error::new("unavailable", "drain prevents new switches"));
            }
            let change =
                store.reserve_switch(&principal_name(principal), &request, &policy, now_ms())?;
            self.publish_locked(&change.events);
            // A disconnected caller cannot cancel the independently scheduled operation.
            if !change.events.is_empty() {
                self.schedule_command(request.id.clone(), None);
            }
            change
        };
        self.signal_progress();
        serde_json::to_value(change.value)
            .map_err(|_| Error::new("internal", "cannot encode switch"))
    }

    pub(super) async fn process_switch(&self, id: &str) {
        let work = {
            let mut store = self.inner.store.lock().await;
            match store.start_switch(id, now_ms()) {
                Ok(change) => {
                    self.publish_locked(&change.events);
                    change.value
                }
                Err(_) => None,
            }
        };
        let Some((request, plan)) = work else {
            return;
        };
        let prepared = async {
            let source = {
                let store = self.inner.store.lock().await;
                store
                    .get_agent(&request.from)?
                    .ok_or_else(|| Error::new("not_found", "source Agent missing"))?
            };
            // Continue-in-project resolves the target checkout before any
            // preparation: an omitted workspace uses the target
            // registration's checkout, never the source repo. The
            // registration the worker verifies here is rechecked inside the
            // admission transaction, so a revocation between preparation and
            // admission refuses instead of dispatching under stale authority.
            let target = match request.launch.project.as_deref() {
                Some(project) => Some({
                    let store = self.inner.store.lock().await;
                    store
                        .get_project_registration("owner", project)?
                        .ok_or_else(|| {
                            Error::new(
                                "not_found",
                                format!("switch target project {project} was not found"),
                            )
                        })?
                }),
                None => None,
            };
            let mut launch = SubmitCommand::new(&plan.launch_command, "launch");
            let workspace = match (
                &target,
                request.launch.workspace.as_deref(),
                source.workspace.as_deref(),
            ) {
                (Some(_), Some(workspace), _) => workspace.to_owned(),
                (Some(registration), None, _) => registration.checkout.clone(),
                (None, workspace, retained) => workspace
                    .or(retained)
                    .ok_or_else(|| {
                        Error::new(
                            "invalid",
                            "switch requires a workspace when A has no retained workspace",
                        )
                    })?
                    .to_owned(),
            };
            launch.args = request.launch.args(&workspace)?;
            // A targeted continuation explicitly requests standard worktree
            // preparation, even in a `shared` project: B never starts in the
            // checkout itself.
            if target.is_some() && request.launch.worktree.is_none() {
                launch.args["worktree"] = json!("");
            }
            let scope = source.project_scope().to_owned();
            // A switch that requests neither a workspace nor a branch keeps
            // the source checkout: reuse is selected before any preparation.
            // A targeted continue never reuses the source checkout: B always
            // runs from the target project through ordinary preparation.
            if target.is_none() && reuse_source_checkout(&request.launch) {
                launch.args["switch_reuse_source_checkout"] = json!(true);
            }
            if let Some(registration) = &target {
                let git = crate::workspace_git::WorkspaceGit::new()?;
                let (checkout, common_dir) = git.repository_identity(workspace.as_str()).await?;
                if checkout != registration.checkout || common_dir != registration.repo_common_dir {
                    return Err(Error::new(
                        "not_allowed",
                        "switch workspace is outside the target project repository",
                    ));
                }
            }
            let mut prepared = self
                .prepare_submission_after_authorization(launch, &Principal::Owner, false, false)
                .await?;
            if target.is_none()
                && reuse_retained_checkout(request.launch.workspace.as_deref(), &prepared.workspace)
            {
                prepared.workspace = None;
                if let Some(args) = prepared.request.args.as_object_mut() {
                    args.remove("workspace_preparation");
                }
            }
            // The replacement retains the source's authorization namespace for handoff.
            prepared.request.args["project"] = json!(scope);
            if let Some(target) = &target {
                // The prepared launch must carry the resolved target
                // registration; an explicit workspace in another repository
                // resolves elsewhere and is refused here.
                if prepared
                    .request
                    .args
                    .get("registered_project")
                    .and_then(Value::as_str)
                    != Some(target.id.as_str())
                {
                    return Err(Error::new(
                        "not_allowed",
                        "switch target project differs from prepared registration",
                    ));
                }
                if prepared
                    .request
                    .args
                    .get("workspace")
                    .and_then(Value::as_str)
                    != Some(target.checkout.as_str())
                {
                    return Err(Error::new(
                        "not_allowed",
                        "switch workspace differs from its target registration",
                    ));
                }
                if let Some(workspace) = prepared.workspace.as_ref()
                    && (workspace.repo_path != target.checkout
                        || workspace.repo_common_dir != target.repo_common_dir
                        || workspace.project != target.checkout)
                {
                    return Err(Error::new(
                        "not_allowed",
                        "switch workspace preparation differs from its target registration",
                    ));
                }
                let current = {
                    let store = self.inner.store.lock().await;
                    store
                        .get_project_registration("owner", &target.id)?
                        .ok_or_else(|| {
                            Error::new(
                                "not_found",
                                format!("switch target project {} was not found", target.id),
                            )
                        })?
                };
                if current.revision != target.revision {
                    return Err(Error::new(
                        "stale_assignment",
                        "switch target registration changed",
                    ));
                }
                prepared.request.args["registered_project"] = json!(target.id);
                prepared.request.args[SWITCH_PROJECT_REVISION_ARG] = json!(target.revision);
            } else {
                prepared.request.args["registered_project"] = json!(source.project);
            }
            if scope.is_empty() {
                prepared.request.args[SWITCH_BLANK_SCOPE_ARG] = json!(true);
            }
            let policy = self.secret_policy_snapshot();
            if policy.contains_value(&prepared.request.args) {
                return Err(Error::new(
                    "secret_in_payload",
                    "switch launch contains a known secret",
                ));
            }
            let mut store = self.inner.store.lock().await;
            self.check_command_admission_mode("launch")?;
            let admission =
                store.admit_switch_launch(id, prepared.request, prepared.workspace, now_ms())?;
            self.publish_locked(&admission.events);
            Ok::<_, Error>(admission)
        }
        .await;
        let admission = match prepared {
            Ok(admission) => admission,
            Err(error) => {
                let mut store = self.inner.store.lock().await;
                if let Ok(change) = store.fail_switch_preparation(id, &error.code, now_ms()) {
                    self.publish_locked(&change.events);
                }
                self.signal_progress();
                return;
            }
        };
        // The same ordinary worker owns native dispatch and settlement. No linked launch
        // prompt is admitted: only H2 can create B's first SEND.
        let mut progress = self.inner.progress.subscribe();
        self.clone()
            .process_command(admission.command.id, None)
            .await;
        // Native ready batches can race the start reply. Wait on committed progress,
        // never infer readiness from Applied alone or from an Owner assertion.
        let _ = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                {
                    let store = self.inner.store.lock().await;
                    let value = store.switch_result(id)?;
                    if value.launch_state != Some(CommandState::Applied)
                        || store.switch_launch_ready(id)?
                    {
                        return Ok::<_, Error>(());
                    }
                }
                if progress.changed().await.is_err() {
                    return Ok(());
                }
            }
        })
        .await;
        let policy = self.secret_policy_snapshot();
        let result = {
            let mut store = self.inner.store.lock().await;
            let result = store.finish_switch(id, &policy, now_ms());
            if let Ok(change) = &result {
                self.publish_locked(&change.events);
                if let Some(handoff) = &change.value.handoff {
                    for child in &handoff.children {
                        self.schedule_command(child.clone(), None);
                    }
                }
            }
            result
        };
        self.signal_progress();
        if let Ok(change) = result
            && let Some(handoff) = change.value.handoff
        {
            self.notify_claim_conflicts(&handoff.claim_conflicts).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn prepared_input(
        generated: Option<eliot_core::GeneratedWorktreeNaming>,
    ) -> WorkspacePreparationInput {
        WorkspacePreparationInput {
            project: "synthetic".to_owned(),
            repo_path: "synthetic".to_owned(),
            repo_common_dir: "synthetic/.git".to_owned(),
            requested_branch: "atlas/a1-task".to_owned(),
            base_ref: Some("main".to_owned()),
            base_oid: "d".repeat(40),
            worktree_path: "synthetic-worktree".to_owned(),
            frozen_setup: Vec::new(),
            generated,
        }
    }

    #[test]
    fn switch_without_a_workspace_or_branch_request_reuses_before_preparation() {
        let bare = eliot_core::api::SwitchLaunch {
            profile: Some("worker".to_owned()),
            harness: None,
            model: None,
            effort: None,
            auth: None,
            cmd: None,
            access: None,
            project: None,
            workspace: None,
            worktree: None,
            base: None,
        };
        assert!(reuse_source_checkout(&bare));
        let branched = eliot_core::api::SwitchLaunch {
            worktree: Some("feature/manual".to_owned()),
            ..bare.clone()
        };
        assert!(!reuse_source_checkout(&branched));
        let moved = eliot_core::api::SwitchLaunch {
            workspace: Some("/elsewhere".to_owned()),
            ..bare.clone()
        };
        assert!(!reuse_source_checkout(&moved));
    }

    #[test]
    fn switch_without_a_workspace_override_reuses_the_retained_checkout() {
        let minted = prepared_input(Some(eliot_core::GeneratedWorktreeNaming {
            project_slug: "atlas".to_owned(),
            task_slug: "task".to_owned(),
            branch: None,
        }));
        assert!(reuse_retained_checkout(None, &Some(minted)));
        // An explicit override keeps its preparation, as does an explicitly
        // requested branch prepared from the retained checkout.
        assert!(!reuse_retained_checkout(Some("/elsewhere"), &None));
        assert!(!reuse_retained_checkout(None, &None));
        let explicit = prepared_input(Some(eliot_core::GeneratedWorktreeNaming {
            project_slug: "atlas".to_owned(),
            task_slug: "task".to_owned(),
            branch: Some("feature/manual".to_owned()),
        }));
        assert!(!reuse_retained_checkout(None, &Some(explicit)));
        assert!(!reuse_retained_checkout(
            Some("/elsewhere"),
            &Some(prepared_input(None))
        ));
    }

    #[tokio::test]
    async fn targeted_continuation_requests_standard_worktree_in_shared_project_and_keeps_canonical_checkout()
     {
        let root = std::env::temp_dir()
            .canonicalize()
            .unwrap()
            .join(format!("eliot-k185b-shared-continue-{}", ulid::Ulid::new()));
        let _guard = crate::test_support::TempDir::own(root.clone());
        let checkout_path = root.join("checkout");
        let data_root = root.join("data");
        std::fs::create_dir_all(&checkout_path).unwrap();
        std::fs::create_dir_all(&data_root).unwrap();
        let git = |args: &[&str]| {
            let output = std::process::Command::new("git")
                .args(args)
                .current_dir(&checkout_path)
                .output()
                .unwrap();
            assert!(output.status.success(), "git {args:?} failed");
        };
        git(&["init", "--quiet", "--initial-branch=main"]);
        git(&["config", "user.name", "ELIOT test"]);
        git(&["config", "user.email", "eliot-test@example.invalid"]);
        git(&["commit", "--quiet", "--allow-empty", "-m", "base"]);
        let (checkout, repo_common_dir) = crate::workspace_git::WorkspaceGit::new()
            .unwrap()
            .repository_identity(&checkout_path.to_string_lossy())
            .await
            .unwrap();
        std::fs::write(
            data_root.join("config.toml"),
            format!(
                "[profiles.fixture]\nharness = \"codex\"\nmodel = \"fixture-model\"\n[projects.shared_app]\npath = {checkout:?}\nworkspace = \"shared\"\nprofiles = [\"fixture\"]\nactions = [\"launch\"]\n"
            ),
        )
        .unwrap();
        let router = super::super::tests::router_with_data_root(data_root);
        {
            let mut store = router.inner.store.lock().await;
            store
                .upsert_project_registration(
                    "owner",
                    &ProjectRegistrationInput {
                        id: "shared_app".to_owned(),
                        checkout: checkout.clone(),
                        repo_common_dir: repo_common_dir.clone(),
                        git: None,
                        profiles: vec!["fixture".to_owned()],
                        actions: vec!["launch".to_owned()],
                        interpreters: std::collections::BTreeMap::new(),
                        run_env: Vec::new(),
                    },
                    now_ms(),
                )
                .unwrap();
        }
        let launch_spec = eliot_core::api::SwitchLaunch {
            profile: Some("fixture".to_owned()),
            harness: None,
            model: None,
            effort: None,
            auth: None,
            cmd: None,
            access: None,
            project: Some("shared_app".to_owned()),
            workspace: None,
            worktree: None,
            base: None,
        };
        let mut launch = SubmitCommand::new(ulid::Ulid::new().to_string(), "launch");
        launch.args = launch_spec.args(&checkout).unwrap();
        launch.args["worktree"] = json!("");
        let prepared = router
            .prepare_submission_after_authorization(launch, &Principal::Owner, false, false)
            .await
            .unwrap();
        let workspace = prepared
            .workspace
            .expect("shared project continuation still prepares a standard worktree");
        assert_eq!(workspace.project, checkout);
        assert_eq!(workspace.repo_path, checkout);
        assert_eq!(workspace.repo_common_dir, repo_common_dir);
        assert_eq!(
            prepared.request.args["registered_project"].as_str(),
            Some("shared_app")
        );
    }

    #[tokio::test]
    async fn ordinary_worktree_switch_after_chat_continue_resolves_retained_base_and_freezes_checkout()
     {
        let root = std::env::temp_dir()
            .canonicalize()
            .unwrap()
            .join(format!("eliot-k185c-switch-wt-{}", ulid::Ulid::new()));
        let _guard = crate::test_support::TempDir::own(root.clone());
        let checkout_path = root.join("checkout");
        let data_root = root.join("data");
        std::fs::create_dir_all(&checkout_path).unwrap();
        std::fs::create_dir_all(&data_root).unwrap();
        let git = |dir: &std::path::Path, args: &[&str]| -> String {
            let output = std::process::Command::new("git")
                .args(args)
                .current_dir(dir)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "git {args:?} failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            String::from_utf8_lossy(&output.stdout).trim().to_owned()
        };
        git(
            &checkout_path,
            &["init", "--quiet", "--initial-branch=main"],
        );
        git(&checkout_path, &["config", "user.name", "ELIOT test"]);
        git(
            &checkout_path,
            &["config", "user.email", "eliot-test@example.invalid"],
        );
        git(
            &checkout_path,
            &["commit", "--quiet", "--allow-empty", "-m", "main-base"],
        );
        let main_head = git(&checkout_path, &["rev-parse", "HEAD"]);
        let (checkout, repo_common_dir) = crate::workspace_git::WorkspaceGit::new()
            .unwrap()
            .repository_identity(&checkout_path.to_string_lossy())
            .await
            .unwrap();
        std::fs::write(
            data_root.join("config.toml"),
            format!(
                "[profiles.fixture]\nharness = \"codex\"\nmodel = \"fixture-model\"\n[projects.app]\npath = {checkout:?}\nprofiles = [\"fixture\"]\nactions = [\"launch\"]\n"
            ),
        )
        .unwrap();
        let router = super::super::tests::router_with_data_root(data_root.clone());
        let revision = {
            let mut store = router.inner.store.lock().await;
            store
                .upsert_project_registration(
                    "owner",
                    &ProjectRegistrationInput {
                        id: "app".to_owned(),
                        checkout: checkout.clone(),
                        repo_common_dir: repo_common_dir.clone(),
                        git: None,
                        profiles: vec!["fixture".to_owned()],
                        actions: vec!["launch".to_owned()],
                        interpreters: std::collections::BTreeMap::new(),
                        run_env: Vec::new(),
                    },
                    now_ms(),
                )
                .unwrap()
                .value
                .revision
        };

        // 1. Start a chat Agent (A1) and continue it into project `app` (A2).
        let (a1, a1_task) = {
            let mut store = router.inner.store.lock().await;
            let mut launch = SubmitCommand::new(ulid::Ulid::new().to_string(), "launch");
            launch.args = json!({
                "chat": true,
                "project": "",
                "workspace": "chat-root",
                "harness": "codex",
                "profile": "fixture",
                "profile_snapshot": {"control": "tree", "model": "fixture-model"},
            });
            let admission = store.submit("owner", launch, now_ms()).unwrap();
            let a1 = admission.command.target.unwrap();
            store.dispatch(&admission.command.id, now_ms()).unwrap();
            crate::test_support::install_fixture_sources(
                &mut store,
                &a1,
                &admission.command.id,
                1,
                now_ms(),
            );
            store
                .put_batch(
                    eliot_core::SourceBatch {
                        batch_id: ulid::Ulid::new().to_string(),
                        agent_id: a1.clone(),
                        generation: 1,
                        source: "stream".into(),
                        cursor: None,
                        events: vec![eliot_core::NativeEvent {
                            ts: None,
                            kind: "session.ready".into(),
                            native_id: "a1-ready".into(),
                            data: json!({"command_id": admission.command.id}),
                        }],
                    },
                    now_ms(),
                )
                .unwrap();
            store
                .settle(&admission.command.id, CommandState::Applied, None, now_ms())
                .unwrap();
            let mut send = SubmitCommand::new(ulid::Ulid::new().to_string(), "send");
            send.target = Some(a1.clone());
            send.expected_generation = Some(1);
            send.args = json!({"text": "continue into app"});
            let task = store.submit("owner", send, now_ms()).unwrap().command.id;
            (a1, task)
        };
        let retained_path = data_root.join("worktrees").join("app").join("a2-retained");
        let a2_prep = eliot_core::WorkspacePreparationInput {
            project: checkout.clone(),
            repo_path: checkout.clone(),
            repo_common_dir: repo_common_dir.clone(),
            requested_branch: "app/a2-retained".to_owned(),
            base_ref: Some("HEAD".to_owned()),
            base_oid: main_head.clone(),
            worktree_path: retained_path.to_string_lossy().into_owned(),
            frozen_setup: Vec::new(),
            generated: None,
        };
        let switch1_id = ulid::Ulid::new().to_string();
        let (a2, a2_task) = {
            let mut store = router.inner.store.lock().await;
            let switch1 = SwitchRequest {
                id: switch1_id.clone(),
                from: a1,
                from_generation: 1,
                task_command: a1_task.clone(),
                commands: vec![a1_task],
                launch: serde_json::from_value(json!({"profile": "fixture", "project": "app"}))
                    .unwrap(),
            };
            let plan = store
                .reserve_switch(
                    "owner",
                    &switch1,
                    &eliot_core::SecretPolicy::default(),
                    now_ms(),
                )
                .unwrap()
                .value;
            store.start_switch(&switch1_id, now_ms()).unwrap();
            let mut launch = SubmitCommand::new(plan.launch_command.clone(), "launch");
            launch.args = json!({
                "project": "",
                "workspace": checkout,
                "harness": "codex",
                "profile": "fixture",
                "profile_snapshot": {"control": "tree", "model": "fixture-model"},
                "registered_project": "app",
                SWITCH_PROJECT_REVISION_ARG: revision,
                SWITCH_BLANK_SCOPE_ARG: true,
            });
            let admission = store
                .admit_switch_launch(&switch1_id, launch, Some(a2_prep.clone()), now_ms())
                .unwrap();
            let a2 = admission.command.target.unwrap();
            (a2, plan.launch_command)
        };
        router.inner.workspaces.prepare(&a2_task).await.unwrap();
        let (a2_send_id, retained_head) = {
            let mut store = router.inner.store.lock().await;
            store.dispatch(&a2_task, now_ms()).unwrap();
            crate::test_support::install_fixture_sources(&mut store, &a2, &a2_task, 1, now_ms());
            store
                .put_batch(
                    eliot_core::SourceBatch {
                        batch_id: ulid::Ulid::new().to_string(),
                        agent_id: a2.clone(),
                        generation: 1,
                        source: "stream".into(),
                        cursor: None,
                        events: vec![eliot_core::NativeEvent {
                            ts: None,
                            kind: "session.ready".into(),
                            native_id: "a2-ready".into(),
                            data: json!({"command_id": a2_task}),
                        }],
                    },
                    now_ms(),
                )
                .unwrap();
            store
                .settle(&a2_task, CommandState::Applied, None, now_ms())
                .unwrap();
            store
                .finish_switch(&switch1_id, &eliot_core::SecretPolicy::default(), now_ms())
                .unwrap();
            // Advance A2's retained worktree one commit past `app`'s primary checkout.
            git(&retained_path, &["config", "user.name", "ELIOT test"]);
            git(
                &retained_path,
                &["config", "user.email", "eliot-test@example.invalid"],
            );
            git(
                &retained_path,
                &["commit", "--quiet", "--allow-empty", "-m", "a2-work"],
            );
            let retained_head = git(&retained_path, &["rev-parse", "HEAD"]);
            assert_ne!(retained_head, main_head);
            let mut send = SubmitCommand::new(ulid::Ulid::new().to_string(), "send");
            send.target = Some(a2.clone());
            send.expected_generation = Some(1);
            send.args = json!({"text": "next step"});
            let send_id = store.submit("owner", send, now_ms()).unwrap().command.id;
            (send_id, retained_head)
        };

        // 2. Ordinary switch of A2 with `--worktree feature/next` and no `--project`.
        let switch2_id = ulid::Ulid::new().to_string();
        let switch2 = SwitchRequest {
            id: switch2_id.clone(),
            from: a2.clone(),
            from_generation: 1,
            task_command: a2_send_id.clone(),
            commands: vec![a2_send_id],
            launch: serde_json::from_value(
                json!({"profile": "fixture", "worktree": "feature/next"}),
            )
            .unwrap(),
        };
        let plan2 = {
            let mut store = router.inner.store.lock().await;
            let plan = store
                .reserve_switch(
                    "owner",
                    &switch2,
                    &eliot_core::SecretPolicy::default(),
                    now_ms(),
                )
                .unwrap()
                .value;
            store.start_switch(&switch2_id, now_ms()).unwrap();
            plan
        };
        let mut launch2 = SubmitCommand::new(plan2.launch_command.clone(), "launch");
        launch2.args = switch2
            .launch
            .args(&retained_path.to_string_lossy())
            .unwrap();
        let mut prepared2 = router
            .prepare_submission_after_authorization(launch2, &Principal::Owner, false, false)
            .await
            .unwrap();
        let prep2 = prepared2
            .workspace
            .as_ref()
            .expect("ordinary switch with --worktree prepares a worktree");
        assert_eq!(prep2.project, checkout);
        assert_eq!(prep2.repo_path, checkout);
        assert_eq!(prep2.repo_common_dir, repo_common_dir);
        assert_eq!(prep2.base_oid, retained_head);
        assert_eq!(prep2.requested_branch, "feature/next");

        prepared2.request.args["project"] = json!("");
        prepared2.request.args["registered_project"] = json!("app");
        prepared2.request.args[SWITCH_BLANK_SCOPE_ARG] = json!(true);
        let a3 = {
            let mut store = router.inner.store.lock().await;
            let admission = store
                .admit_switch_launch(
                    &switch2_id,
                    prepared2.request,
                    prepared2.workspace,
                    now_ms(),
                )
                .expect("admission accepts frozen checkout with retained base commit");
            admission.command.target.unwrap()
        };
        router
            .inner
            .workspaces
            .prepare(&plan2.launch_command)
            .await
            .unwrap();
        let (a3_agent, a3_ws) = {
            let store = router.inner.store.lock().await;
            (
                store.get_agent(&a3).unwrap().unwrap(),
                store.get_workspace(&a3).unwrap().unwrap(),
            )
        };
        assert_eq!(a3_agent.project, "app");
        assert_eq!(a3_agent.project_scope(), "");
        assert_eq!(a3_ws.project, checkout);
        assert_eq!(a3_ws.repo_path, checkout);
        assert_eq!(a3_ws.base_oid, retained_head);
        assert_eq!(a3_ws.requested_branch, "feature/next");
        assert_eq!(
            git(
                std::path::Path::new(&a3_ws.worktree_path),
                &["rev-parse", "HEAD"]
            ),
            retained_head
        );
    }

    #[tokio::test]
    async fn switch_preparation_failure_and_drain_retry_are_retained_without_launch() {
        let router = super::super::tests::router();
        assert_eq!(
            router
                .swarm_switch(
                    &json!({}),
                    &Principal::Agent {
                        id: "a1".into(),
                        generation: 1
                    }
                )
                .await
                .unwrap_err()
                .code,
            "not_allowed"
        );
        let source = {
            let mut store = router.inner.store.lock().await;
            let mut launch = SubmitCommand::new(ulid::Ulid::new().to_string(), "launch");
            launch.args = json!({"project":"missing-h3-workspace","workspace":"missing-h3-workspace","harness":"codex","profile":"worker","profile_snapshot":{"control":"tree"}});
            let admission = store.submit("owner", launch, now_ms()).unwrap();
            let agent = admission.command.target.unwrap();
            store.dispatch(&admission.command.id, now_ms()).unwrap();
            crate::test_support::install_fixture_sources(
                &mut store,
                &agent,
                &admission.command.id,
                1,
                now_ms(),
            );
            store
                .put_batch(
                    eliot_core::SourceBatch {
                        batch_id: ulid::Ulid::new().to_string(),
                        agent_id: agent.clone(),
                        generation: 1,
                        source: "stream".into(),
                        cursor: None,
                        events: vec![eliot_core::NativeEvent {
                            ts: None,
                            kind: "session.ready".into(),
                            native_id: "h3-ready".into(),
                            data: json!({"command_id":admission.command.id}),
                        }],
                    },
                    now_ms(),
                )
                .unwrap();
            store
                .settle(&admission.command.id, CommandState::Applied, None, now_ms())
                .unwrap();
            agent
        };
        let params = json!({"id":ulid::Ulid::new().to_string(),"from":source,"from_generation":1,"task_command":"retained-task","commands":[],"launch":{"profile":"missing-h3-profile"}});
        let mut progress = router.inner.progress.subscribe();
        let first = router
            .swarm_switch(&params, &Principal::Owner)
            .await
            .unwrap();
        assert_eq!(first["state"], "accepted");
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if router
                    .inner
                    .store
                    .lock()
                    .await
                    .switch_result(params["id"].as_str().unwrap())
                    .unwrap()
                    .state
                    == CommandState::Rejected
                {
                    break;
                }
                progress.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
        router.inner.mode.store(MODE_DRAIN, Ordering::SeqCst);
        let retry = router
            .swarm_switch(&params, &Principal::Owner)
            .await
            .unwrap();
        assert_eq!(retry["state"], "rejected");
        assert_eq!(retry["launch_command"], first["launch_command"]);
        assert_eq!(retry["handoff_command"], first["handoff_command"]);
        assert!(retry["created"].is_null());
        assert_eq!(router.inner.store.lock().await.agent_count().unwrap(), 1);
        let mut changed = params;
        changed["launch"]["profile"] = json!("different");
        assert_eq!(
            router
                .swarm_switch(&changed, &Principal::Owner)
                .await
                .unwrap_err()
                .code,
            "id_conflict"
        );
    }
}
