use super::*;
use serde_json::json;

#[test]
fn options_require_explicit_model_bounded_timeout_and_absolute_workdir() {
    // An absolute path on every platform: "/tmp" is not absolute on Windows.
    let abs = std::env::temp_dir().to_string_lossy().into_owned();
    let base = json!({
        "scope_id": "fixture",
        "executable": "eval-cli",
        "workdir": abs,
        "model": "anthropic/claude-sonnet-4-6",
        "timeout_seconds": 60
    });
    assert!(Options::parse(&base).is_ok());
    for broken in [
        json!({"scope_id":"fixture","executable":"eval-cli","workdir":abs,"model":"claude-sonnet-4-6","timeout_seconds":60}),
        json!({"scope_id":"fixture","executable":"eval-cli","workdir":"relative/dir","model":"anthropic/claude-sonnet-4-6","timeout_seconds":60}),
        json!({"scope_id":"fixture","executable":"eval-cli","workdir":abs,"model":"anthropic/claude-sonnet-4-6","timeout_seconds":0}),
        json!({"scope_id":"fixture","executable":"eval-cli","workdir":abs,"model":"anthropic/","timeout_seconds":60}),
        json!({"scope_id":"fixture","executable":"eval-cli","workdir":abs,"model":"anthropic/claude-sonnet-4-6","timeout_seconds":60,"env_keys":["1BAD"]}),
        json!({"scope_id":"fixture","executable":"eval-cli","workdir":abs,"model":"anthropic/claude-sonnet-4-6","timeout_seconds":60,"surprise":true}),
    ] {
        assert_eq!(
            Options::parse(&broken).unwrap_err().code,
            "CONFIG_ERROR",
            "{broken}"
        );
    }
}

#[cfg(unix)]
mod unix {
    use super::super::*;
    use serde_json::json;
    use std::io::Write;
    use std::os::unix::fs::PermissionsExt;

    const SCRIPT: &str = r##"#!/bin/sh
dir=$(dirname "$0")
mode=$(cat "$dir/fake-mode" 2>/dev/null || echo completed)
model=""; out=""; timeout=""
prev=""
for a in "$@"; do
  case "$prev" in
    --model) model="$a" ;;
    --output-dir) out="$a" ;;
    --timeout) timeout="$a" ;;
  esac
  prev="$a"
done
echo "fake eval-cli progress" >&2
case "$mode" in
  completed)
    printf '{"status":"completed","duration_secs":1.25,"timeout_secs":%s,"model":"%s","input_tokens":11,"output_tokens":7,"step_count":3,"tool_call_count":2,"tool_calls":{"edit":2}}' "$timeout" "$model" > "$out/result.json"
    printf '# Thread\n\nhello from fixture\n' > "$out/thread.md"
    printf '{"thread":"fixture"}' > "$out/thread.json"
    exit 0 ;;
  big_thread)
    printf '{"status":"completed","duration_secs":2.5,"timeout_secs":%s,"model":"%s"}' "$timeout" "$model" > "$out/result.json"
    head -c 200000 /dev/zero | tr '\0' 'a' > "$out/thread.md"
    printf '{"thread":"fixture"}' > "$out/thread.json"
    exit 0 ;;
  error)
    printf '{"status":"error","error":"model auth failed","duration_secs":0.5,"timeout_secs":%s,"model":"%s"}' "$timeout" "$model" > "$out/result.json"
    exit 1 ;;
  timeout)
    printf '{"status":"timeout","duration_secs":%s,"timeout_secs":%s,"model":"%s"}' "$timeout" "$timeout" "$model" > "$out/result.json"
    exit 2 ;;
  interrupted)
    printf '{"status":"interrupted","duration_secs":0.25,"timeout_secs":%s,"model":"%s"}' "$timeout" "$model" > "$out/result.json"
    printf 'partial transcript\n' > "$out/thread.md"
    exit 3 ;;
  early_error)
    exit 1 ;;
  mismatch)
    printf '{"status":"error","error":"boom","duration_secs":0.5,"timeout_secs":%s,"model":"%s"}' "$timeout" "$model" > "$out/result.json"
    exit 0 ;;
  model_mismatch)
    printf '{"status":"completed","duration_secs":0.5,"timeout_secs":%s,"model":"other/model"}' "$timeout" > "$out/result.json"
    exit 0 ;;
  missing_result)
    exit 0 ;;
  sleep)
    sleep 30
    exit 0 ;;
esac
exit 9
"##;

    struct Fixture {
        root: PathBuf,
        bin: PathBuf,
        workdir: PathBuf,
        out_root: PathBuf,
        data_dir: PathBuf,
    }
    impl Fixture {
        fn new(mode: &str) -> Self {
            let root = std::env::temp_dir().join(format!("eliot-zed-test-{}", model::new_id()));
            let bin_dir = root.join("bin");
            let workdir = root.join("work");
            std::fs::create_dir_all(&bin_dir).unwrap();
            std::fs::create_dir_all(&workdir).unwrap();
            let bin = bin_dir.join("eval-cli");
            let mut file = File::create(&bin).unwrap();
            file.write_all(SCRIPT.as_bytes()).unwrap();
            std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
            std::fs::write(bin_dir.join("fake-mode"), mode).unwrap();
            Self {
                out_root: root.join("runs"),
                data_dir: root.join("state"),
                root,
                bin,
                workdir,
            }
        }
        fn options(&self, timeout_seconds: u64) -> Options {
            Options::parse(&json!({
                "scope_id": "fixture",
                "executable": self.bin.to_string_lossy(),
                "workdir": self.workdir,
                "model": "anthropic/claude-sonnet-4-6",
                "timeout_seconds": timeout_seconds,
                "env_keys": ["ANTHROPIC_API_KEY"]
            }))
            .unwrap()
        }
        fn run(&self, mode_timeout: u64, operation: &str) -> Result<BatchOutcome> {
            let artifacts = ArtifactFiles::new(&self.data_dir).unwrap();
            run_batch(
                &self.options(mode_timeout),
                operation,
                "Fix the fixture bug",
                &self.out_root,
                &artifacts,
            )
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }

    #[test]
    fn resolve_program_uses_path_and_rejects_missing() {
        let fixture = Fixture::new("completed");
        let path = std::ffi::OsString::from(fixture.bin.parent().unwrap().as_os_str());
        let resolved = resolve_program("eval-cli", Some(&path)).unwrap();
        assert_eq!(resolved, fixture.bin);
        assert_eq!(
            resolve_program("eval-cli-missing", Some(&path))
                .unwrap_err()
                .code,
            "NATIVE_EXECUTABLE_NOT_FOUND"
        );
        assert_eq!(
            resolve_program("/nonexistent/eval-cli", Some(&path))
                .unwrap_err()
                .code,
            "NATIVE_EXECUTABLE_NOT_FOUND"
        );
    }

    #[test]
    fn describe_reports_batch_only_capabilities() {
        let fixture = Fixture::new("completed");
        let facts = describe(&fixture.options(60)).unwrap();
        assert_eq!(facts["entrypoint"], "eval_cli_batch");
        assert_eq!(facts["installed_runtime_verified"], false);
        assert_eq!(facts["model"], "anthropic/claude-sonnet-4-6");
        assert_eq!(facts["capabilities"]["batch_run"], true);
        assert_eq!(facts["capabilities"]["goal"], false);
        assert_eq!(facts["capabilities"]["steer"], false);
        assert_eq!(facts["capabilities"]["persistent_control"], false);
        assert_eq!(facts["capabilities"]["resume"], false);
    }

    #[test]
    fn completed_run_publishes_all_native_outputs() {
        let fixture = Fixture::new("completed");
        let outcome = fixture.run(5, "op-zed-completed").unwrap();
        assert_eq!(outcome.disposition, BatchDisposition::Completed);
        assert_eq!(outcome.exit_code, Some(0));
        assert!(!outcome.host_terminated);
        assert_eq!(outcome.completion_condition, "native_batch_result_recorded");
        let native = outcome.native_result.unwrap();
        assert_eq!(native["status"], "completed");
        assert_eq!(native["usage"]["input_tokens"], 11);
        assert_eq!(native["usage"]["basis"], "native_batch_result");
        assert_eq!(native["step_count"], 3);
        // result.json + thread.md + thread.json, one page each.
        assert_eq!(outcome.artifacts.len(), 3);
        let artifacts = ArtifactFiles::new(&fixture.data_dir).unwrap();
        for record in &outcome.artifacts {
            artifacts.verify(record).unwrap();
            assert_eq!(record.metadata["runtime"], "zed");
        }
        let names: Vec<&str> = outcome
            .artifacts
            .iter()
            .map(|r| r.metadata["native_output"].as_str().unwrap())
            .collect();
        assert_eq!(names, ["result.json", "thread.md", "thread.json"]);
        let result_page = outcome
            .artifacts
            .iter()
            .find(|record| record.metadata["native_output"] == "result.json")
            .unwrap();
        assert_eq!(
            outcome.native_result_sha256.as_deref(),
            Some(result_page.content_digest.as_str())
        );
    }

    #[test]
    fn large_transcript_is_published_in_pages_without_truncation() {
        let fixture = Fixture::new("big_thread");
        let outcome = fixture.run(5, "op-zed-big").unwrap();
        assert_eq!(outcome.disposition, BatchDisposition::Completed);
        // 200_000 bytes over 65_536-byte pages = 4 pages, plus result + thread.json.
        assert_eq!(outcome.artifacts.len(), 6);
        let thread_pages: Vec<&ArtifactRecord> = outcome
            .artifacts
            .iter()
            .filter(|r| r.metadata["native_output"] == "thread.md")
            .collect();
        assert_eq!(thread_pages.len(), 4);
        assert!(thread_pages.iter().all(|r| r.metadata["pages"] == 4));
        let total: u64 = thread_pages.iter().map(|r| r.byte_length).sum();
        assert_eq!(total, 200_000);
    }

    #[test]
    fn native_error_timeout_and_interrupted_are_facts_not_controller_errors() {
        let error = Fixture::new("error");
        let outcome = error.run(5, "op-zed-error").unwrap();
        assert_eq!(outcome.disposition, BatchDisposition::Error);
        assert_eq!(outcome.exit_code, Some(1));
        assert_eq!(
            outcome.native_result.as_ref().unwrap()["error_reported"],
            true
        );
        assert!(
            outcome
                .native_result
                .as_ref()
                .unwrap()
                .get("error")
                .is_none()
        );

        let timeout = Fixture::new("timeout");
        let outcome = timeout.run(5, "op-zed-timeout").unwrap();
        assert_eq!(outcome.disposition, BatchDisposition::Timeout);
        assert_eq!(outcome.exit_code, Some(2));
        assert!(!outcome.host_terminated);

        let interrupted = Fixture::new("interrupted");
        let outcome = interrupted.run(5, "op-zed-interrupted").unwrap();
        assert_eq!(outcome.disposition, BatchDisposition::Interrupted);
        assert_eq!(outcome.exit_code, Some(3));
        // Partial transcript from an interrupted run is still published.
        assert!(
            outcome
                .artifacts
                .iter()
                .any(|r| r.metadata["native_output"] == "thread.md")
        );
    }

    #[test]
    fn early_failure_without_result_is_classified_by_exit_only() {
        let fixture = Fixture::new("early_error");
        let outcome = fixture.run(5, "op-zed-early").unwrap();
        assert_eq!(outcome.disposition, BatchDisposition::Error);
        assert!(outcome.native_result.is_none());
        assert!(outcome.artifacts.is_empty());
        assert_eq!(outcome.completion_condition, "native_batch_exit_classified");
    }

    #[test]
    fn contradictory_native_evidence_is_rejected() {
        let mismatch = Fixture::new("mismatch");
        assert_eq!(
            mismatch.run(5, "op-zed-mismatch").unwrap_err().code,
            "NATIVE_RESULT_MISMATCH"
        );
        let model_mismatch = Fixture::new("model_mismatch");
        assert_eq!(
            model_mismatch
                .run(5, "op-zed-model-mismatch")
                .unwrap_err()
                .code,
            "NATIVE_RESULT_MISMATCH"
        );
        let missing = Fixture::new("missing_result");
        assert_eq!(
            missing.run(5, "op-zed-missing").unwrap_err().code,
            "NATIVE_RESULT_MISSING"
        );
    }

    #[test]
    fn hung_binary_is_terminated_at_the_host_deadline() {
        let fixture = Fixture::new("sleep");
        let outcome = fixture.run(1, "op-zed-sleep").unwrap();
        assert_eq!(outcome.disposition, BatchDisposition::Timeout);
        assert!(outcome.host_terminated);
        assert!(outcome.native_result.is_none());
    }

    #[test]
    fn run_identity_cannot_be_reused() {
        let fixture = Fixture::new("completed");
        fixture.run(5, "op-zed-once").unwrap();
        assert_eq!(fixture.run(5, "op-zed-once").unwrap_err().code, "CONFLICT");
    }

    #[test]
    fn terminal_receipt_binds_artifacts_to_exact_operation_and_frozen_prompt() {
        let fixture = Fixture::new("big_thread");
        let options = fixture.options(5);
        let route = json!({
            "runtime":RUNTIME,
            "module_artifact_id":ARTIFACT_ID,
            "native_options":{
                "scope_id":"fixture",
                "executable":fixture.bin,
                "workdir":fixture.workdir,
                "model":"anthropic/claude-sonnet-4-6",
                "timeout_seconds":5,
                "env_keys":["ANTHROPIC_API_KEY"]
            }
        });
        let input = json!({
            "text":"Fix the fixture bug",
            "task_snapshot":{"task_id":"task-1","revision":4,"requirements":[]}
        });
        let command = RuntimeCommand {
            operation_id: "op-zed-receipt".into(),
            method: "task.dispatch".into(),
            created_at_ms: 1,
            binding_id: "binding-1".into(),
            generation: 1,
            native_root_id: None,
            route: route.clone(),
            input: input.clone(),
            input_sha256: None,
            target_input_sha256: None,
        };
        let instruction = crate::runtime::batch::instruction(&input).unwrap();
        let artifact_files = ArtifactFiles::new(&fixture.data_dir).unwrap();
        let (batch, intent) = run_batch_command(
            &options,
            &command,
            &instruction,
            &fixture.out_root,
            &artifact_files,
        )
        .unwrap();
        fn sync_manifest(value: &mut Value) {
            let artifacts = value["artifacts"].as_array().unwrap();
            let refs = artifacts
                .iter()
                .map(|record| record["artifact_id"].clone())
                .collect::<Vec<_>>();
            let mut outputs = serde_json::Map::new();
            for record in artifacts {
                let output = record["metadata"]["native_output"].as_str().unwrap();
                outputs
                    .entry(output.to_owned())
                    .or_insert_with(|| json!([]))
                    .as_array_mut()
                    .unwrap()
                    .push(record["artifact_id"].clone());
            }
            value["outcome"]["details"]["artifact_refs"] = json!(refs);
            value["outcome"]["details"]["output_artifact_refs"] = json!(outputs);
        }

        let artifact_refs = batch
            .artifacts
            .iter()
            .map(|record| json!(record.artifact_id))
            .collect::<Vec<_>>();
        let mut output_artifact_refs = serde_json::Map::new();
        for record in &batch.artifacts {
            let output = record.metadata["native_output"].as_str().unwrap();
            let refs = output_artifact_refs
                .entry(output.to_owned())
                .or_insert_with(|| json!([]));
            refs.as_array_mut().unwrap().push(json!(record.artifact_id));
        }
        let receipt = BatchReceipt {
            version: 1,
            intent: intent.clone(),
            outcome: RuntimeOutcome {
                operation_id: command.operation_id.clone(),
                outcome: crate::runtime::EffectOutcome::Applied,
                native_scope_key: None,
                native_root_id: None,
                turn_id: None,
                native_input_id: None,
                details: json!({
                    "execution_shape":crate::runtime::batch::EXECUTION_SHAPE,
                    "completion_condition":"native_result_observed",
                    "batch_run_id":intent.run_id,
                    "requested_model":"anthropic/claude-sonnet-4-6",
                    "effective_model":"anthropic/claude-sonnet-4-6",
                    "effective_model_status":"observed",
                    "exit_code":batch.exit_code,
                    "native_result":batch.native_result,
                    "native_result_sha256":batch.native_result_sha256,
                    "artifact_refs":artifact_refs,
                    "output_artifact_refs":output_artifact_refs
                }),
            },
            artifacts: batch.artifacts,
        };
        persist_receipt(&fixture.out_root, &artifact_files, &route, &receipt).unwrap();
        persist_receipt(&fixture.out_root, &artifact_files, &route, &receipt).unwrap();
        let saved = read_receipt(
            &fixture.out_root,
            BatchReadContext {
                operation_id: &command.operation_id,
                binding_id: &command.binding_id,
                generation: command.generation,
                route: &route,
                instruction: &instruction,
                task_snapshot: &input["task_snapshot"],
            },
            &artifact_files,
        )
        .unwrap()
        .unwrap();
        assert_eq!(saved.intent, intent);
        assert_eq!(saved.artifacts.len(), 6);

        let terminal_path =
            run_directory(&fixture.out_root, &command.operation_id).join("terminal.json");
        let reject_tampered_receipt = |value: Value| {
            let malformed: BatchReceipt = serde_json::from_value(value.clone()).unwrap();
            assert_eq!(
                persist_receipt(&fixture.out_root, &artifact_files, &route, &malformed)
                    .unwrap_err()
                    .code,
                "BATCH_RECEIPT_MISMATCH"
            );
            std::fs::write(&terminal_path, model::canonical(&value).unwrap()).unwrap();
            assert_eq!(
                read_receipt(
                    &fixture.out_root,
                    BatchReadContext {
                        operation_id: &command.operation_id,
                        binding_id: &command.binding_id,
                        generation: command.generation,
                        route: &route,
                        instruction: &instruction,
                        task_snapshot: &input["task_snapshot"],
                    },
                    &artifact_files,
                )
                .unwrap_err()
                .code,
                "BATCH_RECEIPT_MISMATCH"
            );
        };
        let original = serde_json::to_value(&receipt).unwrap();
        assert!(!original["outcome"]["details"]["native_result"].is_null());
        let mut wrong_result_digest = original.clone();
        wrong_result_digest["outcome"]["details"]["native_result_sha256"] = json!("0".repeat(64));
        reject_tampered_receipt(wrong_result_digest);

        let mut wrong_projection = original.clone();
        wrong_projection["outcome"]["details"]["native_result"]["model"] =
            json!("provider/other-model");
        reject_tampered_receipt(wrong_projection);

        let mut wrong_run = original.clone();
        wrong_run["outcome"]["details"]["batch_run_id"] = json!("foreign-run");
        reject_tampered_receipt(wrong_run);

        let mut wrong_ref = original.clone();
        wrong_ref["outcome"]["details"]["artifact_refs"]
            .as_array_mut()
            .unwrap()
            .push(json!("unregistered-page"));
        reject_tampered_receipt(wrong_ref);

        let mut wrong_group = original.clone();
        wrong_group["outcome"]["details"]["output_artifact_refs"]["result.json"] =
            json!(["unregistered-page"]);
        reject_tampered_receipt(wrong_group);

        let mut missing_page = original.clone();
        missing_page["artifacts"]
            .as_array_mut()
            .unwrap()
            .retain(|record| {
                !(record["metadata"]["native_output"] == "thread.md"
                    && record["metadata"]["page"] == 3)
            });
        sync_manifest(&mut missing_page);
        reject_tampered_receipt(missing_page);

        let mut missing_result = original.clone();
        missing_result["artifacts"]
            .as_array_mut()
            .unwrap()
            .retain(|record| record["metadata"]["native_output"] != "result.json");
        sync_manifest(&mut missing_result);
        reject_tampered_receipt(missing_result);

        let mut relabelled_pages = original.clone();
        let artifacts = relabelled_pages["artifacts"].as_array_mut().unwrap();
        let result_index = artifacts
            .iter()
            .position(|record| record["metadata"]["native_output"] == "result.json")
            .unwrap();
        let json_thread_index = artifacts
            .iter()
            .position(|record| record["metadata"]["native_output"] == "thread.json")
            .unwrap();
        artifacts[result_index]["metadata"]["native_output"] = json!("thread.json");
        artifacts[json_thread_index]["metadata"]["native_output"] = json!("result.json");
        sync_manifest(&mut relabelled_pages);
        reject_tampered_receipt(relabelled_pages);

        std::fs::write(&terminal_path, model::canonical(&original).unwrap()).unwrap();
        assert_eq!(
            read_receipt(
                &fixture.out_root,
                BatchReadContext {
                    operation_id: &command.operation_id,
                    binding_id: &command.binding_id,
                    generation: command.generation,
                    route: &route,
                    instruction: "different frozen text",
                    task_snapshot: &input["task_snapshot"],
                },
                &artifact_files,
            )
            .unwrap_err()
            .code,
            "BATCH_RECEIPT_MISMATCH"
        );
    }
}
