//! One-shot process metrics for identities explicitly selected by the caller.
//!
//! This path is separate from Store-backed observer snapshots. It accepts only
//! bounded private receipt files, never enumerates processes, and never starts,
//! stops, or changes a process. The caller must check the existing trusted
//! `observability.enabled` gate before any receipt is opened.

use crate::process_metrics::ProcessRole;
#[cfg(windows)]
use crate::process_metrics::{
    ObservedProcess, ProcessMetricsSampler, ProcessMetricsSnapshot, ProcessResourceSample,
    SnapshotStatus,
};
use serde_json::{Value, json};
use std::path::Path;
#[cfg(windows)]
use swarm_contracts::error::Error;
use swarm_contracts::error::Result;

#[cfg(windows)]
const MAX_IMAGE_RECEIPT_BYTES: u64 = 64 * 1024;
#[cfg(windows)]
const MAX_OWNER_RECEIPT_BYTES: u64 = 64 * 1024;
#[cfg(windows)]
const MAX_WORKER_RECEIPT_BYTES: u64 = 64 * 1024;
#[cfg(windows)]
const MAX_EXPLICIT_INTERVAL_MS: u64 = 2_000;
#[cfg(windows)]
const MIN_EXPLICIT_INTERVAL_MS: u64 = 10;

/// Run one explicit process observation, or exactly two explicit observations
/// separated by one bounded interval. The function does no work when disabled
/// and does not read any path in that case.
///
/// `host_identity_path` must name a private JSON file containing the exact
/// four-field Windows receipt returned by `swarm_process::process_image_identity`.
/// The host role is descriptive: there is no host ownership proof in that
/// receipt. A module child requires the existing private `owner.json` and
/// `worker.json` receipts from the same module state directory; current OS
/// membership is checked by `ObservedProcess::module_child` and again for each
/// sample. The helper/adapter label is descriptive and grants no authority.
///
/// The selected paths are caller-owned absolute paths. Each is checked for
/// traversal/reparse links, a regular file, expected receipt shape, and a
/// strict size bound. Receipt paths and private identity data are never emitted.
pub fn run_explicit_metrics(
    enabled: bool,
    host_identity_path: Option<&Path>,
    owner_record_path: Option<&Path>,
    worker_record_path: Option<&Path>,
    child_role: Option<ProcessRole>,
    interval_ms: Option<u64>,
) -> Result<Value> {
    if !enabled {
        return Ok(json!({
            "schema_version": 1,
            "status": "disabled",
            "input_files_read": false,
            "observation_cut": "not_sampled",
            "samples": []
        }));
    }

    #[cfg(not(windows))]
    {
        let _ = (
            host_identity_path,
            owner_record_path,
            worker_record_path,
            child_role,
            interval_ms,
        );
        Ok(json!({
            "schema_version": 1,
            "status": "unavailable",
            "unavailable_reason": "unsupported_platform",
            "input_files_read": false,
            "observation_cut": "process_metrics_unavailable",
            "store_readback": "not_performed",
            "native_family_coverage": "not_claimed",
            "samples": []
        }))
    }

    #[cfg(windows)]
    run_windows(
        host_identity_path,
        owner_record_path,
        worker_record_path,
        child_role,
        interval_ms,
    )
}

#[cfg(windows)]
fn run_windows(
    host_identity_path: Option<&Path>,
    owner_record_path: Option<&Path>,
    worker_record_path: Option<&Path>,
    child_role: Option<ProcessRole>,
    interval_ms: Option<u64>,
) -> Result<Value> {
    use std::{thread, time::Duration};

    if interval_ms.is_some_and(|value| {
        !(MIN_EXPLICIT_INTERVAL_MS..=MAX_EXPLICIT_INTERVAL_MS).contains(&value)
    }) {
        return Err(Error::new(
            "OBSERVER_METRICS_INTERVAL_INVALID",
            "the explicit metrics interval must be between 10 and 2000 milliseconds",
        ));
    }
    let child_parts_present =
        owner_record_path.is_some() || worker_record_path.is_some() || child_role.is_some();
    if child_parts_present
        && (owner_record_path.is_none() || worker_record_path.is_none() || child_role.is_none())
    {
        return Err(input_error());
    }
    if host_identity_path.is_none() && !child_parts_present {
        return Err(Error::new(
            "OBSERVER_METRICS_SELECTION_REQUIRED",
            "select a private host image receipt or a module owner and worker receipt",
        ));
    }
    if matches!(child_role, Some(ProcessRole::Host)) {
        return Err(input_error());
    }

    let mut selected = Vec::with_capacity(2);
    let mut construction_failures = Vec::with_capacity(2);
    if let Some(path) = host_identity_path {
        let image = read_receipt(path, MAX_IMAGE_RECEIPT_BYTES, None)?;
        validate_image_receipt(&image)?;
        match ObservedProcess::host(&image) {
            Ok(subject) => selected.push(subject),
            Err(reason) => construction_failures.push(ProcessResourceSample::unavailable(
                ProcessRole::Host,
                None,
                None,
                reason,
            )),
        }
    }

    if let (Some(owner_path), Some(worker_path), Some(role)) =
        (owner_record_path, worker_record_path, child_role)
    {
        let (owner, owner_canonical) =
            read_receipt_with_path(owner_path, MAX_OWNER_RECEIPT_BYTES, Some("owner.json"))?;
        let (worker, worker_canonical) =
            read_receipt_with_path(worker_path, MAX_WORKER_RECEIPT_BYTES, Some("worker.json"))?;
        if owner_canonical.parent() != worker_canonical.parent() {
            return Err(input_error());
        }
        validate_owner_receipt(&owner)?;
        let image = worker_image_receipt(&worker)?;
        match ObservedProcess::module_child(role, &owner, &image) {
            Ok(subject) => selected.push(subject),
            Err(reason) => construction_failures
                .push(ProcessResourceSample::unavailable(role, None, None, reason)),
        }
    }

    let mut sampler = ProcessMetricsSampler::default();
    let first = merge_construction_failures(
        sampler.sample_explicit_tick(true, &selected),
        &construction_failures,
    );
    let mut snapshots = vec![safe_snapshot_value(first)?];
    if let Some(interval_ms) = interval_ms {
        // One explicit wait at most; this command never becomes a polling loop.
        thread::sleep(Duration::from_millis(interval_ms));
        let second = merge_construction_failures(
            sampler.sample_explicit_tick(true, &selected),
            &construction_failures,
        );
        snapshots.push(safe_snapshot_value(second)?);
    }

    let measurement_status = if snapshots
        .iter()
        .all(|snapshot| snapshot.get("status").and_then(Value::as_str) == Some("unavailable"))
    {
        "unavailable"
    } else if snapshots.iter().any(|snapshot| {
        matches!(
            snapshot.get("status").and_then(Value::as_str),
            Some("partial" | "unavailable")
        )
    }) {
        "partial"
    } else {
        "observed"
    };
    let host_provenance =
        host_identity_path.map(|_| "caller_supplied_exact_image_receipt; role_descriptive_only");
    let child_provenance = child_role
        .map(|_| "same_directory_owner_and_worker_receipts; membership_check_result_in_samples");
    Ok(json!({
        "schema_version": 1,
        "status": measurement_status,
        "sampling": if interval_ms.is_some() { "two_explicit_samples" } else { "one_explicit_sample" },
        "interval_ms": interval_ms,
        "observation_cut": "independent_process_only_not_atomic_with_store",
        "store_readback": "not_performed",
        "native_family_coverage": "not_claimed",
        "identity_provenance": {
            "host": host_provenance,
            "module_child": child_provenance,
            "receipt_paths_emitted": false
        },
        "snapshots": snapshots
    }))
}

#[cfg(windows)]
fn merge_construction_failures(
    mut snapshot: ProcessMetricsSnapshot,
    failures: &[ProcessResourceSample],
) -> ProcessMetricsSnapshot {
    if failures.is_empty() {
        return snapshot;
    }
    snapshot.samples.extend_from_slice(failures);
    snapshot.status = match snapshot.status {
        SnapshotStatus::Empty => SnapshotStatus::Unavailable,
        SnapshotStatus::Observed => SnapshotStatus::Partial,
        status => status,
    };
    snapshot
}

#[cfg(windows)]
fn safe_snapshot_value(snapshot: ProcessMetricsSnapshot) -> Result<Value> {
    let mut value = serde_json::to_value(snapshot).map_err(|_| {
        Error::new(
            "OBSERVER_METRICS_OUTPUT_INVALID",
            "process metrics could not be rendered as bounded metadata",
        )
    })?;
    if let Some(samples) = value.get_mut("samples").and_then(Value::as_array_mut) {
        for sample in samples {
            let Some(object) = sample.as_object_mut() else {
                return Err(Error::new(
                    "OBSERVER_METRICS_OUTPUT_INVALID",
                    "process metrics sample is outside the supported schema",
                ));
            };
            for key in [
                "pid",
                "process_birth_filetime",
                "resident_bytes",
                "cumulative_cpu_100ns",
                "cpu_percent",
                "cpu_interval_ns",
                "logical_processor_count",
                "sampled_at_unix_ms",
                "unavailable_reason",
            ] {
                object.entry(key).or_insert(Value::Null);
            }
            if object.get("status").and_then(Value::as_str) == Some("unavailable") {
                object.insert("cpu_percent".into(), Value::Null);
            }
        }
    }
    Ok(value)
}

#[cfg(windows)]
fn read_receipt(path: &Path, limit: u64, expected_name: Option<&str>) -> Result<Value> {
    read_receipt_with_path(path, limit, expected_name).map(|(value, _)| value)
}

#[cfg(windows)]
fn read_receipt_with_path(
    path: &Path,
    limit: u64,
    expected_name: Option<&str>,
) -> Result<(Value, std::path::PathBuf)> {
    use std::{
        fs::{self, File},
        io::Read,
        path::Component,
    };

    if !path.is_absolute()
        || path
            .components()
            .any(|part| matches!(part, Component::CurDir | Component::ParentDir))
        || expected_name
            .is_some_and(|name| path.file_name().and_then(|value| value.to_str()) != Some(name))
    {
        return Err(input_error());
    }
    reject_link_components(path)?;
    let canonical = fs::canonicalize(path).map_err(|_| input_error())?;
    let metadata = fs::metadata(&canonical).map_err(|_| input_error())?;
    if !metadata.is_file() || metadata.len() > limit {
        return Err(input_error());
    }
    let mut file = File::open(&canonical).map_err(|_| input_error())?;
    let handle_metadata = file.metadata().map_err(|_| input_error())?;
    if !handle_metadata.is_file() || handle_metadata.len() > limit {
        return Err(input_error());
    }
    let mut body = Vec::with_capacity(handle_metadata.len() as usize);
    (&mut file)
        .take(limit + 1)
        .read_to_end(&mut body)
        .map_err(|_| input_error())?;
    if body.len() as u64 > limit {
        return Err(input_error());
    }
    let value: Value = serde_json::from_slice(&body).map_err(|_| input_error())?;
    Ok((value, canonical))
}

#[cfg(windows)]
fn reject_link_components(path: &Path) -> Result<()> {
    use std::{
        fs,
        path::{Component, PathBuf},
    };
    let mut current = PathBuf::new();
    for component in path.components() {
        if matches!(component, Component::CurDir | Component::ParentDir) {
            return Err(input_error());
        }
        current.push(component.as_os_str());
        if matches!(component, Component::Prefix(_)) {
            continue;
        }
        let metadata = fs::symlink_metadata(&current).map_err(|_| input_error())?;
        if metadata.file_type().is_symlink() || is_reparse(&metadata) {
            return Err(input_error());
        }
    }
    Ok(())
}

#[cfg(windows)]
fn is_reparse(metadata: &std::fs::Metadata) -> bool {
    use std::os::windows::fs::MetadataExt;
    metadata.file_attributes() & 0x400 != 0
}

#[cfg(windows)]
fn validate_image_receipt(image: &Value) -> Result<()> {
    let object = image.as_object().ok_or_else(input_error)?;
    if object.len() != 4
        || ["pid", "creation_filetime", "image_path", "image_sha256"]
            .iter()
            .any(|key| !object.contains_key(*key))
        || !object
            .get("pid")
            .and_then(Value::as_u64)
            .is_some_and(|pid| pid > 0 && pid <= u64::from(u32::MAX))
    {
        return Err(input_error());
    }
    let creation = object
        .get("creation_filetime")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty() && value.len() <= 20)
        .and_then(|value| {
            value
                .parse::<u64>()
                .ok()
                .filter(|parsed| parsed.to_string() == value)
        });
    let image_path = object
        .get("image_path")
        .and_then(Value::as_str)
        .filter(|value| {
            !value.is_empty()
                && value.len() <= 32 * 1024
                && !value.chars().any(char::is_control)
                && Path::new(value).is_absolute()
        });
    let hash = object
        .get("image_sha256")
        .and_then(Value::as_str)
        .and_then(|value| value.strip_prefix("sha256:"));
    if creation.is_none()
        || image_path.is_none()
        || !hash.is_some_and(|value| {
            value.len() == 64
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })
    {
        return Err(input_error());
    }
    Ok(())
}

#[cfg(windows)]
fn validate_owner_receipt(owner: &Value) -> Result<()> {
    let object = owner.as_object().ok_or_else(input_error)?;
    if object.len() != 3
        || ["version", "token", "process"]
            .iter()
            .any(|key| !object.contains_key(*key))
        || object.get("version").and_then(Value::as_u64) != Some(1)
        || object.get("token").and_then(Value::as_str).is_none()
        || !object.get("process").is_some_and(Value::is_object)
    {
        return Err(input_error());
    }
    Ok(())
}

#[cfg(windows)]
fn worker_image_receipt(worker: &Value) -> Result<Value> {
    let object = worker.as_object().ok_or_else(input_error)?;
    let expected = [
        "version",
        "boot_id",
        "module",
        "binding",
        "generation",
        "artifact_id",
        "artifact_version",
        "build_id",
        "protocol",
        "module_contract",
        "module_client_id",
        "process",
    ];
    if object.len() != expected.len()
        || expected.iter().any(|key| !object.contains_key(*key))
        || object.get("version").and_then(Value::as_u64) != Some(1)
    {
        return Err(input_error());
    }
    for key in [
        "boot_id",
        "module",
        "binding",
        "generation",
        "artifact_id",
        "artifact_version",
        "protocol",
        "module_contract",
        "module_client_id",
    ] {
        if !object
            .get(key)
            .and_then(Value::as_str)
            .is_some_and(|value| !value.is_empty() && value.len() <= 1024)
        {
            return Err(input_error());
        }
    }
    if !object.get("build_id").is_some_and(|value| {
        value.is_null()
            || value.as_str().is_some_and(|text| {
                !text.is_empty() && text.len() <= 1024 && !text.chars().any(char::is_control)
            })
    }) {
        return Err(input_error());
    }
    let image = object.get("process").ok_or_else(input_error)?.clone();
    validate_image_receipt(&image)?;
    Ok(image)
}

#[cfg(windows)]
fn input_error() -> Error {
    Error::new(
        "OBSERVER_METRICS_INPUT_INVALID",
        "a selected private process receipt is outside the bounded supported schema",
    )
}

#[cfg(all(test, windows))]
mod tests {
    use super::*;

    #[test]
    fn bootstrap_path_canonical_metrics_receipt_is_readable() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("observer-path-{}-{nonce}.json", std::process::id()));
        drop(
            std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
                .unwrap(),
        );
        swarm_process::private_permissions(&path, false).unwrap();
        reject_link_components(&path).unwrap();
        reject_link_components(&std::fs::canonicalize(&path).unwrap()).unwrap();
        std::fs::remove_file(path).unwrap();
    }
}
