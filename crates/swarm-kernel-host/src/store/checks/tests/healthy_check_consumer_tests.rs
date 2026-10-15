use super::{Fixture, create_attempt, run_check_process};
use crate::{
    checks::{
        inputs,
        model::{CheckProfile, Parser},
        worker,
    },
    config::Config,
    error::{Error, Result},
    model::{self, Principal, Role},
    platform::{DataRoot, bootstrap_credential},
    store::StoreOwner,
};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::Arc,
};

struct FixtureDirectory(PathBuf);

impl Drop for FixtureDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn healthy_profile() -> CheckProfile {
    CheckProfile {
        profile_id: "native-healthy-check".into(),
        profile_revision: "v1".into(),
        executable: "node".into(),
        args: vec![
            "-e".into(),
            "process.stdout.write('healthy-check-child\\n')".into(),
        ],
        parser: Parser::ExitCode,
        resource: format!("healthy-check-{}", model::new_id()),
        environment: BTreeMap::new(),
        inherit_env: Vec::new(),
        expected_targets: Vec::new(),
        reproducible: false,
        fingerprint_env: Vec::new(),
        versioned_inputs: BTreeMap::new(),
    }
}

fn actual_node_version_probe(
    program: &Path,
    args: &[String],
    env: &BTreeMap<String, String>,
    cwd: &Path,
) -> Result<Vec<u8>> {
    let output = Command::new(program)
        .args(args)
        .current_dir(cwd)
        .env_clear()
        .envs(env)
        .stdin(Stdio::null())
        .output()?;
    if !output.status.success() || output.stdout.len() > 1024 * 1024 {
        return Err(Error::new(
            "TEST_NODE_VERSION_PROBE",
            "the configured Node version probe failed or exceeded its output bound",
        ));
    }
    Ok(output.stdout)
}

fn git(repository: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .args(args)
        .current_dir(repository)
        .output()
        .expect("run private source-capture fixture Git command");
    assert!(
        output.status.success(),
        "Git {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .expect("Git fixture output is UTF-8")
        .trim()
        .to_owned()
}

async fn fixture_with_node_profile() -> (FixtureDirectory, Fixture, CheckProfile) {
    let directory = std::env::temp_dir().join(format!("swarm-check-healthy-{}", model::new_id()));
    fs::create_dir_all(&directory).unwrap();
    let root = DataRoot::acquire(&directory).unwrap();
    let directory = root.path.clone();
    let cleanup = FixtureDirectory(directory.clone());
    let credential = bootstrap_credential(&directory).unwrap();
    let profile = healthy_profile();
    let mut config = Config::default();
    config.storage.data_dir = directory.clone();
    config.checks.enabled = true;
    config.checks.profiles = vec![profile.clone()];
    let config = Arc::new(config);
    let owner = StoreOwner::start(root, config.clone(), credential.clone())
        .await
        .unwrap();
    let principal = owner.store.authenticate(credential).await.unwrap();
    let writer = Principal {
        link_id: model::new_id(),
        client_id: format!("healthy-check-writer-{}", model::new_id()),
        role: Role::Manager,
    };
    let writer_id = writer.client_id.clone();
    owner
        .store
        .run(move |db| {
            super::super::set_meta(
                db,
                &format!("client:{writer_id}"),
                &json!({"role":Role::Manager,"disabled":false}),
            )
        })
        .await
        .unwrap();
    (
        cleanup,
        Fixture {
            directory,
            owner,
            principal,
            writer,
            config,
        },
        profile,
    )
}

fn assert_birth_identity(process: &Value) {
    assert!(
        process["pid"].as_u64().is_some(),
        "process identity: {process}"
    );
    #[cfg(windows)]
    assert!(
        process["creation_filetime"]
            .as_u64()
            .is_some_and(|value| value > 0),
        "Windows process birth identity: {process}"
    );
    #[cfg(target_os = "linux")]
    assert!(
        process["start_ticks"]
            .as_str()
            .is_some_and(|value| !value.is_empty()),
        "Linux process birth identity: {process}"
    );
}

#[tokio::test]
async fn public_check_run_consumes_a_healthy_native_child_receipt() {
    let (_cleanup, fixture, profile) = fixture_with_node_profile().await;
    let store = &fixture.owner.store;
    let (_task_id, attempt_id) = create_attempt(&fixture, "native-healthy-check", None).await;

    let repository = fixture.directory.join("healthy-check-source");
    fs::create_dir_all(&repository).unwrap();
    git(&repository, &["init", "--quiet"]);
    git(&repository, &["config", "user.name", "Fixture Author"]);
    git(
        &repository,
        &["config", "user.email", "fixture@example.invalid"],
    );
    fs::write(
        repository.join("source.js"),
        b"// captured check candidate\n",
    )
    .unwrap();
    git(&repository, &["add", "source.js"]);
    git(
        &repository,
        &["commit", "--quiet", "-m", "healthy check source"],
    );
    let commit = git(&repository, &["rev-parse", "HEAD"]);
    let capture = store
        .call(
            fixture.principal.clone(),
            "source.capture".into(),
            json!({
                "client_request_id":model::new_id(),
                "attempt_id":attempt_id,
                "expected_revision":1,
                "repository":repository,
                "commit":commit,
            }),
        )
        .await
        .unwrap();
    let capture_operation = store
        .call(
            fixture.principal.clone(),
            "operation.get".into(),
            json!({"operation_id":capture["operation_id"]}),
        )
        .await
        .unwrap();
    assert_eq!(capture_operation["state"], "settled");
    assert_eq!(capture_operation["method"], "source.capture");
    assert_eq!(capture_operation["result"]["outcome"], "applied");
    let candidate_ref = capture_operation["result"]["candidate_ref"]
        .as_str()
        .expect("public source.capture produced the candidate")
        .to_owned();

    let _probe = inputs::override_probe_for_test(&profile, actual_node_version_probe);
    let admission = store
        .call(
            fixture.principal.clone(),
            "check.run".into(),
            json!({
                "client_request_id":model::new_id(),
                "attempt_id":attempt_id,
                "candidate_ref":candidate_ref,
                "profile_id":profile.profile_id,
                "profile_revision":profile.profile_revision,
            }),
        )
        .await
        .unwrap();
    assert_eq!(admission["state"], "queued");
    let check_id = admission["check_id"]
        .as_str()
        .expect("public check.run produced the CheckRun")
        .to_owned();
    let operation_id = admission["operation_id"].as_str().unwrap().to_owned();

    // The existing Store consumer obtains Work from `next`, runs the compiled
    // kernel CheckRun worker, acknowledges its exact worker identity, validates
    // its Completion, and publishes the normal public `check.get` result.
    run_check_process(&fixture, &check_id).await;

    let status = store
        .call(
            fixture.principal.clone(),
            "check.get".into(),
            json!({"check_id":check_id}),
        )
        .await
        .unwrap();
    assert_eq!(status["state"], "passed");
    assert_eq!(status["exit_code"], 0);
    assert!(status["resource_released_at_ms"].as_i64().is_some());

    let directory = worker::directory(&fixture.directory, &check_id).unwrap();
    let execution: Value = serde_json::from_slice(
        &fs::read(directory.join("execution.json")).expect("durable execution receipt"),
    )
    .unwrap();
    assert_eq!(execution["version"], 1);
    assert_eq!(execution["check_id"], check_id);
    assert_eq!(execution["operation_id"], operation_id);
    let execution_facts = &execution["execution"];
    let facts = &execution_facts["process_facts"];
    assert!(execution_facts["child_pid"].as_u64().is_some());
    assert_eq!(execution_facts["exit_code"], 0);
    assert_eq!(facts["termination"], "exited");
    assert_eq!(facts["direct_exit"]["state"], "observed");
    assert_eq!(facts["direct_exit"]["exit_code"], 0);
    assert_eq!(facts["family_departure"]["state"], "confirmed");
    assert_eq!(facts["resource_released"], true);
    assert_eq!(facts["cleanup_pending"], false);
    let family_process = &facts["family_departure"]["process"];
    assert_eq!(family_process, &execution["process"]);
    assert_birth_identity(family_process);

    let expected_stdout = b"healthy-check-child\n";
    assert_eq!(fs::read(directory.join("stdout")).unwrap(), expected_stdout);
    assert_eq!(facts["stdout_capture"]["capture_complete"], true);
    assert_eq!(
        facts["stdout_capture"]["bytes_observed"],
        expected_stdout.len()
    );

    let operation = store
        .call(
            fixture.principal.clone(),
            "operation.get".into(),
            json!({"operation_id":operation_id}),
        )
        .await
        .unwrap();
    assert_eq!(operation["state"], "settled");
    assert_eq!(operation["result"]["outcome"], "applied");
    let result_ref = operation["result"]["result_ref"]
        .as_str()
        .expect("CheckRun producer retained its result artifact");
    let result: Value = serde_json::from_slice(
        &fs::read(
            fixture
                .directory
                .join("artifacts")
                .join(format!("{result_ref}.bin")),
        )
        .expect("retained CheckRun result artifact"),
    )
    .unwrap();
    assert_eq!(result["candidate_ref"], candidate_ref);
    assert_eq!(result["state"], "passed");
    assert_eq!(result["exit_code"], 0);
    assert_eq!(result["source_checkout_verified"], true);
    assert_eq!(result["resource_released"], true);
    assert_eq!(&result["process_facts"], facts);

    let output_paths = super::output_paths(&fixture, &operation_id).await;
    assert!(
        output_paths
            .iter()
            .any(|path| { fs::read(path).is_ok_and(|bytes| bytes == expected_stdout) }),
        "the public CheckRun result must retain the actual Node stdout artifact"
    );

    fixture.owner.close().await.unwrap();
}
