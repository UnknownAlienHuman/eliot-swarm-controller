//! Verification for the installed OpenCode server dependency tree.
//!
//! The installer writes the closure manifest only after a locked `npm ci` and
//! pins its exact bytes in the package install receipt. This module checks the
//! receipt, package sidecars, five source-pinned resource files, and every
//! regular file below `node_modules` before a caller uses the installed tree.

use sha2::{Digest, Sha256};
use std::{
    fs::{self, File},
    io::Read,
    path::{Component, Path, PathBuf},
};

const RECEIPT_FORMAT: &str = "eliot.frontend_install_receipt.v1";
const BUILD_MANIFEST_FORMAT: &str = "eliot.module_build_manifest.v1";
const CLOSURE_FORMAT: &str = "eliot.opencode_dependency_closure.v1";
const CLOSURE_FILE: &str = "dependency-closure.json";
const INSTALLED_RESOURCE_ROOT: &str = "resources/modules/opencode";
const MAX_RECEIPT_BYTES: u64 = 4 * 1024 * 1024;
const MAX_BUILD_MANIFEST_BYTES: u64 = 16 * 1024 * 1024;
const MAX_CLOSURE_MANIFEST_BYTES: u64 = 32 * 1024 * 1024;
const MAX_CLOSURE_FILES: u64 = 100_000;
const MAX_CLOSURE_ENTRIES: u64 = 200_000;
const MAX_CLOSURE_TOTAL_BYTES: u64 = 1024 * 1024 * 1024;
const MAX_RESOURCE_FILE_BYTES: u64 = 64 * 1024 * 1024;
const REQUIRED_RESOURCE_FILES: &[(&str, &str)] = &[
    ("serve.mjs", "server_program"),
    ("native-mcp-proof.mjs", "plugin_module"),
    ("index.mjs", "plugin_entry"),
    ("package.json", "dependency_manifest"),
    ("package-lock.json", "dependency_lock"),
];

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifiedDependencyClosure {
    pub resource_root: PathBuf,
    pub manifest_sha256: String,
    pub package_lock_sha256: String,
    pub tree_sha256: String,
    pub node_version: String,
    pub node_executable_sha256: String,
    pub npm_version: String,
    pub npm_cli_sha256: String,
    pub entry_count: u64,
    pub file_count: u64,
    pub total_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct FileEntry {
    path: String,
    bytes: u64,
    sha256: String,
}

/// Verifies an installed OpenCode resource closure and returns its canonical root.
///
/// `install_root` is the directory containing `<binary_target>.exe`, its build
/// manifest, and its install receipt. The names and coordinates are supplied by
/// the caller and checked against the receipt; adjacent JSON alone is never
/// treated as authority.
pub fn verify_installed_dependency_closure(
    install_root: &Path,
    expected_package_name: &str,
    expected_binary_target: &str,
    expected_coordinate: &str,
) -> Result<VerifiedDependencyClosure, String> {
    if expected_package_name != "swarm-kernel-host"
        || expected_binary_target != "swarm-kernel-host"
        || expected_coordinate != "swarm-kernel-host-opencode-resources"
        || !safe_coordinate(expected_package_name)
        || !safe_coordinate(expected_binary_target)
        || !safe_coordinate(expected_coordinate)
    {
        return Err("installed dependency closure coordinate is malformed".to_owned());
    }
    let install_root = canonical_directory(install_root)?;
    let receipt_path = install_root.join(format!("{expected_binary_target}.install-receipt.json"));
    let receipt_bytes = read_regular_bounded(&receipt_path, MAX_RECEIPT_BYTES)?;
    let receipt: serde_json::Value = serde_json::from_slice(&receipt_bytes)
        .map_err(|_| "installed package receipt is not valid JSON".to_owned())?;
    require_u64(&receipt, "schema_version", 1)?;
    require_text(&receipt, "format", RECEIPT_FORMAT)?;
    require_text(&receipt, "role", "host_runtime")?;
    require_text(&receipt, "package_name", expected_package_name)?;
    require_text(
        &receipt,
        "package_manifest",
        "crates/swarm-kernel-host/Cargo.toml",
    )?;
    require_text(&receipt, "binary_target", expected_binary_target)?;

    let binary_file = field_text(&receipt, "binary_file")?;
    let expected_binary_file = format!("{expected_binary_target}{}", std::env::consts::EXE_SUFFIX);
    if !safe_leaf(binary_file) || binary_file != expected_binary_file {
        return Err("installed package receipt has an unsafe executable path".to_owned());
    }
    let executable_path = install_root.join(binary_file);
    let executable_sha256 = field_hash(&receipt, "executable_sha256")?;
    if sha256_file(&executable_path, MAX_CLOSURE_TOTAL_BYTES)? != executable_sha256 {
        return Err("installed executable does not match its package receipt".to_owned());
    }

    let build_manifest_file = field_text(&receipt, "build_manifest_file")?;
    let expected_build_manifest_file = format!("{expected_binary_target}.build-manifest.json");
    if !safe_leaf(build_manifest_file) || build_manifest_file != expected_build_manifest_file {
        return Err("installed package receipt has an unsafe build-manifest path".to_owned());
    }
    let build_manifest_path = install_root.join(build_manifest_file);
    let build_manifest_sha256 = field_hash(&receipt, "build_manifest_sha256")?;
    let build_manifest_bytes =
        read_regular_bounded(&build_manifest_path, MAX_BUILD_MANIFEST_BYTES)?;
    if sha256_bytes(&build_manifest_bytes) != build_manifest_sha256 {
        return Err("installed build manifest does not match its package receipt".to_owned());
    }
    let build_manifest: serde_json::Value = serde_json::from_slice(&build_manifest_bytes)
        .map_err(|_| "installed build manifest is not valid JSON".to_owned())?;
    require_u64(&build_manifest, "schema_version", 1)?;
    require_text(&build_manifest, "format", BUILD_MANIFEST_FORMAT)?;
    let build = field(&build_manifest, "build")?;
    require_text(build, "package_name", expected_package_name)?;
    require_text(build, "role", "host_runtime")?;
    require_text(
        build,
        "package_manifest",
        "crates/swarm-kernel-host/Cargo.toml",
    )?;
    require_text(build, "profile", "release")?;
    let binary_targets = field(build, "binary_targets")?
        .as_array()
        .ok_or_else(|| "installed build manifest has no binary target list".to_owned())?;
    if binary_targets.len() != 1 || binary_targets[0].as_str() != Some(expected_binary_target) {
        return Err("installed build manifest names a different executable".to_owned());
    }
    let artifacts = field(&build_manifest, "artifacts")?
        .as_array()
        .ok_or_else(|| "installed build manifest has no artifact list".to_owned())?;
    let artifact = artifacts
        .first()
        .filter(|_| artifacts.len() == 1)
        .ok_or_else(|| "installed build manifest has an unexpected artifact set".to_owned())?;
    let expected_artifact_file = format!("bin/{binary_file}");
    let executable_bytes = fs::metadata(&executable_path)
        .map_err(|_| "installed executable metadata is unavailable".to_owned())?
        .len();
    if field_text(artifact, "target_name")? != expected_binary_target
        || field_text(artifact, "file")? != expected_artifact_file
        || field_hash(artifact, "source_sha256")? != executable_sha256
        || field_hash(artifact, "artifact_sha256")? != executable_sha256
        || field_u64(artifact, "bytes")? != executable_bytes
    {
        return Err("installed executable does not match its build artifact record".to_owned());
    }
    let source = field(&build_manifest, "source")?;
    require_text(
        source,
        "package_manifest_path",
        "crates/swarm-kernel-host/Cargo.toml",
    )?;
    let package_manifest_sha256 = field_hash(source, "package_manifest_sha256")?;
    let workspace_manifest_sha256 = field_hash(source, "workspace_cargo_toml_sha256")?;
    if field_hash(source, "cargo_toml_sha256")? != workspace_manifest_sha256
        || field_hash(&receipt, "package_manifest_sha256")? != package_manifest_sha256
        || field_hash(&receipt, "workspace_cargo_toml_sha256")? != workspace_manifest_sha256
        || field_hash(&receipt, "cargo_toml_sha256")? != workspace_manifest_sha256
    {
        return Err("installed package manifest pins do not match the build source".to_owned());
    }

    let resources = field(&receipt, "resources")?;
    require_text(resources, "coordinate", expected_coordinate)?;
    require_text(
        resources,
        "installed_relative_root",
        INSTALLED_RESOURCE_ROOT,
    )?;
    let resource_root = install_root.join(INSTALLED_RESOURCE_ROOT);
    let resource_root = canonical_directory(&resource_root)?;
    let build_resources = field(&build_manifest, "resources")?;
    require_text(build_resources, "coordinate", expected_coordinate)?;
    require_text(
        build_resources,
        "installed_relative_root",
        INSTALLED_RESOURCE_ROOT,
    )?;
    require_text(
        build_resources,
        "repository_relative_root",
        "modules/opencode",
    )?;
    let dependency_policy = field(build_resources, "dependency_policy")?;
    require_text(
        dependency_policy,
        "node_modules",
        "installer_generated_locked_closure",
    )?;
    require_text(
        dependency_policy,
        "install_command",
        "npm ci --ignore-scripts",
    )?;
    require_text(dependency_policy, "closure_manifest_file", CLOSURE_FILE)?;
    require_text(dependency_policy, "manager", "npm")?;
    require_text(dependency_policy, "output_directory", "node_modules")?;
    require_u64(dependency_policy, "timeout_seconds", 1800)?;
    let arguments = field(dependency_policy, "arguments")?
        .as_array()
        .ok_or_else(|| "installed dependency policy has no npm argument list".to_owned())?;
    let expected_arguments = [
        "ci",
        "--ignore-scripts",
        "--no-audit",
        "--no-fund",
        "--no-progress",
        "--loglevel=error",
    ];
    if arguments.len() != expected_arguments.len()
        || arguments
            .iter()
            .zip(expected_arguments)
            .any(|(actual, expected)| actual.as_str() != Some(expected))
    {
        return Err("installed dependency policy has unsupported npm arguments".to_owned());
    }
    if field_u64(dependency_policy, "max_files")? != MAX_CLOSURE_FILES
        || field_u64(dependency_policy, "max_entries")? != MAX_CLOSURE_ENTRIES
        || field_u64(dependency_policy, "max_total_bytes")? != MAX_CLOSURE_TOTAL_BYTES
        || field_u64(dependency_policy, "max_manifest_bytes")? != MAX_CLOSURE_MANIFEST_BYTES
    {
        return Err("installed dependency closure policy has unsupported bounds".to_owned());
    }

    verify_resource_files(resources, build_resources, &resource_root)?;

    let closure_pin = field(resources, "dependency_closure")?;
    require_u64(closure_pin, "schema_version", 1)?;
    require_text(closure_pin, "manifest_file", CLOSURE_FILE)?;
    require_text(closure_pin, "coordinate", expected_coordinate)?;
    let manifest_sha256 = field_hash(closure_pin, "manifest_sha256")?;
    let closure_path = resource_root.join(CLOSURE_FILE);
    let closure_bytes = read_regular_bounded(&closure_path, MAX_CLOSURE_MANIFEST_BYTES)?;
    if sha256_bytes(&closure_bytes) != manifest_sha256 {
        return Err("dependency closure manifest does not match the install receipt".to_owned());
    }
    let closure: serde_json::Value = serde_json::from_slice(&closure_bytes)
        .map_err(|_| "dependency closure manifest is not valid JSON".to_owned())?;
    require_u64(&closure, "schema_version", 1)?;
    require_text(&closure, "format", CLOSURE_FORMAT)?;
    require_text(&closure, "coordinate", expected_coordinate)?;
    let package_json_sha256 = field_hash(&closure, "package_json_sha256")?;
    let package_lock_sha256 = field_hash(&closure, "package_lock_sha256")?;
    let tree_sha256 = field_hash(&closure, "tree_sha256")?;
    if hash_regular_resource(&resource_root, "package.json")? != package_json_sha256
        || hash_regular_resource(&resource_root, "package-lock.json")? != package_lock_sha256
    {
        return Err("OpenCode package files differ from the locked dependency closure".to_owned());
    }

    let node = field(&closure, "node")?;
    let node_version = field_text(node, "version")?.to_owned();
    validate_version(&node_version)?;
    let node_executable_sha256 = field_hash(node, "executable_sha256")?;
    let node_file = field_text(node, "file")?;
    if !safe_leaf(node_file) {
        return Err("dependency closure has an unsafe Node executable identity".to_owned());
    }
    let npm = field(&closure, "npm")?;
    let npm_version = field_text(npm, "version")?.to_owned();
    validate_version(&npm_version)?;
    let npm_cli_sha256 = field_hash(npm, "cli_sha256")?;
    let npm_cli_file = field_text(npm, "file")?;
    if !safe_leaf(npm_cli_file) {
        return Err("dependency closure has an unsafe npm CLI identity".to_owned());
    }

    let expected_summary = [
        ("package_json_sha256", package_json_sha256.as_str()),
        ("package_lock_sha256", package_lock_sha256.as_str()),
        ("tree_sha256", tree_sha256.as_str()),
        ("node_version", node_version.as_str()),
        ("node_executable_sha256", node_executable_sha256.as_str()),
        ("node_file", node_file),
        ("npm_version", npm_version.as_str()),
        ("npm_cli_sha256", npm_cli_sha256.as_str()),
        ("npm_cli_file", npm_cli_file),
    ];
    for (key, expected) in expected_summary {
        require_text(closure_pin, key, expected)?;
    }

    let files = parse_file_entries(&closure)?;
    let file_count = field_u64(&closure, "file_count")?;
    let entry_count = field_u64(&closure, "entry_count")?;
    let total_bytes = field_u64(&closure, "total_bytes")?;
    if file_count != files.len() as u64
        || file_count == 0
        || file_count > MAX_CLOSURE_FILES
        || entry_count < file_count
        || entry_count > MAX_CLOSURE_ENTRIES
        || total_bytes > MAX_CLOSURE_TOTAL_BYTES
        || files
            .iter()
            .try_fold(0_u64, |sum, file| sum.checked_add(file.bytes))
            != Some(total_bytes)
    {
        return Err("dependency closure manifest exceeds its declared bounds".to_owned());
    }
    if field_u64(closure_pin, "entry_count")? != entry_count
        || field_u64(closure_pin, "file_count")? != file_count
        || field_u64(closure_pin, "total_bytes")? != total_bytes
    {
        return Err("dependency closure counts differ from the install receipt".to_owned());
    }
    let computed_tree_sha256 = tree_digest(&files)?;
    if computed_tree_sha256 != tree_sha256 {
        return Err("dependency closure tree digest is invalid".to_owned());
    }
    let observed_entry_count = verify_node_modules(&resource_root, &files, total_bytes)?;
    if observed_entry_count != entry_count {
        return Err(
            "installed node_modules entry count differs from the closure manifest".to_owned(),
        );
    }

    Ok(VerifiedDependencyClosure {
        resource_root,
        manifest_sha256,
        package_lock_sha256,
        tree_sha256,
        node_version,
        node_executable_sha256,
        npm_version,
        npm_cli_sha256,
        entry_count,
        file_count,
        total_bytes,
    })
}

fn verify_resource_files(
    receipt_resources: &serde_json::Value,
    build_resources: &serde_json::Value,
    root: &Path,
) -> Result<(), String> {
    let receipt_rows = field(receipt_resources, "files")?
        .as_array()
        .ok_or_else(|| "install receipt resource rows are malformed".to_owned())?;
    let build_rows = field(build_resources, "files")?
        .as_array()
        .ok_or_else(|| "build manifest resource rows are malformed".to_owned())?;
    if receipt_rows.len() != REQUIRED_RESOURCE_FILES.len()
        || build_rows.len() != REQUIRED_RESOURCE_FILES.len()
    {
        return Err("OpenCode resource receipt is incomplete".to_owned());
    }
    for (index, (name, role)) in REQUIRED_RESOURCE_FILES.iter().enumerate() {
        let receipt_row = &receipt_rows[index];
        let build_row = &build_rows[index];
        let expected_file = format!("{INSTALLED_RESOURCE_ROOT}/{name}");
        for row in [receipt_row, build_row] {
            if field_text(row, "path")? != *name
                || field_text(row, "file")? != expected_file
                || field_text(row, "role")? != *role
                || field_u64(row, "bytes")? > MAX_RESOURCE_FILE_BYTES
            {
                return Err(
                    "OpenCode resource row does not match the installed coordinate".to_owned(),
                );
            }
            let source_hash = field_hash(row, "source_sha256")?;
            let artifact_hash = field_hash(row, "artifact_sha256")?;
            if source_hash != artifact_hash {
                return Err("OpenCode resource source and artifact pins differ".to_owned());
            }
        }
        if field_hash(receipt_row, "artifact_sha256")? != field_hash(build_row, "artifact_sha256")?
            || field_u64(receipt_row, "bytes")? != field_u64(build_row, "bytes")?
        {
            return Err("OpenCode resource receipt differs from the build manifest".to_owned());
        }
        let actual_path = root.join(name);
        let actual_metadata = fs::metadata(&actual_path)
            .map_err(|_| "installed OpenCode resource metadata is unavailable".to_owned())?;
        let actual = hash_regular_resource(root, name)?;
        if actual != field_hash(receipt_row, "artifact_sha256")?
            || actual_metadata.len() != field_u64(receipt_row, "bytes")?
        {
            return Err("installed OpenCode resource differs from its package receipt".to_owned());
        }
    }
    Ok(())
}

fn parse_file_entries(manifest: &serde_json::Value) -> Result<Vec<FileEntry>, String> {
    let rows = field(manifest, "files")?
        .as_array()
        .ok_or_else(|| "dependency closure file list is malformed".to_owned())?;
    if rows.is_empty() || rows.len() as u64 > MAX_CLOSURE_FILES {
        return Err("dependency closure file list exceeds its bounds".to_owned());
    }
    let mut entries = Vec::with_capacity(rows.len());
    let mut previous: Option<String> = None;
    for row in rows {
        let path = field_text(row, "path")?.to_owned();
        if !safe_relative_path(&path) {
            return Err("dependency closure contains an unsafe relative path".to_owned());
        }
        if previous
            .as_ref()
            .is_some_and(|prior| prior.as_bytes() >= path.as_bytes())
        {
            return Err("dependency closure paths are not uniquely sorted".to_owned());
        }
        let bytes = field_u64(row, "bytes")?;
        if bytes > MAX_CLOSURE_TOTAL_BYTES {
            return Err("dependency closure file exceeds its size bound".to_owned());
        }
        let sha256 = field_hash(row, "sha256")?;
        entries.push(FileEntry {
            path: path.clone(),
            bytes,
            sha256,
        });
        previous = Some(path);
    }
    Ok(entries)
}

fn verify_node_modules(
    root: &Path,
    expected: &[FileEntry],
    expected_total: u64,
) -> Result<u64, String> {
    let node_modules = root.join("node_modules");
    let node_modules = canonical_directory(&node_modules)?;
    if !node_modules.starts_with(root) {
        return Err("installed node_modules directory escaped its resource root".to_owned());
    }
    let mut pending = vec![node_modules.clone()];
    let mut actual = Vec::with_capacity(expected.len());
    let mut total_bytes = 0_u64;
    let mut entry_count = 0_u64;
    while let Some(directory) = pending.pop() {
        let entries = fs::read_dir(&directory)
            .map_err(|_| "installed node_modules directory could not be read".to_owned())?;
        for entry in entries {
            let entry =
                entry.map_err(|_| "installed node_modules entry could not be read".to_owned())?;
            entry_count = entry_count
                .checked_add(1)
                .ok_or_else(|| "installed node_modules entry count overflowed".to_owned())?;
            if entry_count > MAX_CLOSURE_ENTRIES {
                return Err("installed node_modules exceeds its directory-entry bound".to_owned());
            }
            let path = entry.path();
            let metadata = fs::symlink_metadata(&path).map_err(|_| {
                "installed node_modules entry metadata could not be read".to_owned()
            })?;
            if is_reparse_point(&metadata) {
                return Err("installed node_modules contains a link".to_owned());
            }
            let canonical = path
                .canonicalize()
                .map_err(|_| "installed node_modules entry could not be resolved".to_owned())?;
            if !canonical.starts_with(&node_modules) {
                return Err("installed node_modules entry escaped its directory".to_owned());
            }
            if metadata.is_dir() {
                pending.push(path);
                continue;
            }
            if !metadata.is_file() {
                return Err("installed node_modules contains a non-regular entry".to_owned());
            }
            let relative = path
                .strip_prefix(&node_modules)
                .map_err(|_| "installed node_modules path is outside its directory".to_owned())?;
            let relative = relative
                .to_str()
                .ok_or_else(|| "installed node_modules path is not UTF-8".to_owned())?
                .replace('\\', "/");
            if !safe_relative_path(&relative) || actual.len() as u64 >= MAX_CLOSURE_FILES {
                return Err("installed node_modules contains an unsafe or excess file".to_owned());
            }
            let bytes = metadata.len();
            total_bytes = total_bytes
                .checked_add(bytes)
                .ok_or_else(|| "installed node_modules size overflowed".to_owned())?;
            if total_bytes > MAX_CLOSURE_TOTAL_BYTES {
                return Err("installed node_modules exceeds its total size bound".to_owned());
            }
            let sha256 = sha256_file(&path, MAX_CLOSURE_TOTAL_BYTES)?;
            actual.push(FileEntry {
                path: relative,
                bytes,
                sha256,
            });
        }
    }
    actual.sort_by(|left, right| left.path.as_bytes().cmp(right.path.as_bytes()));
    if actual.len() != expected.len() || total_bytes != expected_total {
        return Err("installed node_modules file set differs from its closure manifest".to_owned());
    }
    for (observed, pinned) in actual.iter().zip(expected) {
        if observed != pinned {
            return Err("installed node_modules bytes differ from the closure manifest".to_owned());
        }
    }
    Ok(entry_count)
}

fn tree_digest(files: &[FileEntry]) -> Result<String, String> {
    let mut hasher = Sha256::new();
    let mut previous: Option<&str> = None;
    for entry in files {
        if !safe_relative_path(&entry.path)
            || previous.is_some_and(|prior| prior.as_bytes() >= entry.path.as_bytes())
            || !valid_hash(&entry.sha256)
        {
            return Err("dependency closure rows are not canonical".to_owned());
        }
        hasher.update(entry.path.as_bytes());
        hasher.update([0]);
        hasher.update(entry.bytes.to_string().as_bytes());
        hasher.update([0]);
        hasher.update(entry.sha256.as_bytes());
        hasher.update(b"\n");
        previous = Some(&entry.path);
    }
    let digest = hasher.finalize();
    Ok(hex_lower(&digest))
}

fn hash_regular_resource(root: &Path, name: &str) -> Result<String, String> {
    let path = root.join(name);
    let metadata = fs::symlink_metadata(&path)
        .map_err(|_| "installed OpenCode resource is missing".to_owned())?;
    if !metadata.is_file() || is_reparse_point(&metadata) || metadata.len() == 0 {
        return Err("installed OpenCode resource is not a regular non-empty file".to_owned());
    }
    let canonical = path
        .canonicalize()
        .map_err(|_| "installed OpenCode resource could not be resolved".to_owned())?;
    if !canonical.starts_with(root) || metadata.len() > MAX_RESOURCE_FILE_BYTES {
        return Err(
            "installed OpenCode resource escaped its root or exceeded its bound".to_owned(),
        );
    }
    sha256_file(&canonical, MAX_RESOURCE_FILE_BYTES)
}

fn canonical_directory(path: &Path) -> Result<PathBuf, String> {
    if !path.is_absolute() {
        return Err("installed package directory path is not absolute".to_owned());
    }
    #[cfg(windows)]
    {
        let ancestors = path.ancestors().collect::<Vec<_>>();
        for ancestor in ancestors.into_iter().rev() {
            let metadata = fs::symlink_metadata(ancestor)
                .map_err(|_| "installed package directory path is unavailable".to_owned())?;
            if is_reparse_point(&metadata) {
                return Err(
                    "installed package directory path contains a link or reparse point".to_owned(),
                );
            }
        }
    }
    let metadata = fs::symlink_metadata(path)
        .map_err(|_| "installed package directory is missing".to_owned())?;
    if !metadata.is_dir() || is_reparse_point(&metadata) {
        return Err("installed package directory is not a regular directory".to_owned());
    }
    path.canonicalize()
        .map_err(|_| "installed package directory could not be resolved".to_owned())
}

fn read_regular_bounded(path: &Path, limit: u64) -> Result<Vec<u8>, String> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|_| "installed package sidecar is missing".to_owned())?;
    if !metadata.is_file() || is_reparse_point(&metadata) || metadata.len() > limit {
        return Err("installed package sidecar is not a bounded regular file".to_owned());
    }
    let file =
        File::open(path).map_err(|_| "installed package sidecar could not be opened".to_owned())?;
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(limit + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| "installed package sidecar could not be read".to_owned())?;
    if bytes.len() as u64 > limit || bytes.len() as u64 != metadata.len() {
        return Err("installed package sidecar changed or exceeded its bound".to_owned());
    }
    Ok(bytes)
}

fn sha256_file(path: &Path, limit: u64) -> Result<String, String> {
    let metadata =
        fs::symlink_metadata(path).map_err(|_| "installed package file is missing".to_owned())?;
    if !metadata.is_file() || is_reparse_point(&metadata) || metadata.len() > limit {
        return Err("installed package file is not a bounded regular file".to_owned());
    }
    let mut file =
        File::open(path).map_err(|_| "installed package file could not be opened".to_owned())?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    let mut total = 0_u64;
    loop {
        let count = file
            .read(&mut buffer)
            .map_err(|_| "installed package file could not be hashed".to_owned())?;
        if count == 0 {
            break;
        }
        total = total
            .checked_add(count as u64)
            .ok_or_else(|| "installed package file size overflowed".to_owned())?;
        if total > limit {
            return Err("installed package file exceeded its size bound".to_owned());
        }
        hasher.update(&buffer[..count]);
    }
    if total != metadata.len() {
        return Err("installed package file changed while it was hashed".to_owned());
    }
    let digest = hasher.finalize();
    Ok(hex_lower(&digest))
}

fn sha256_bytes(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    hex_lower(&digest)
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn field<'a>(value: &'a serde_json::Value, name: &str) -> Result<&'a serde_json::Value, String> {
    value
        .as_object()
        .and_then(|object| object.get(name))
        .ok_or_else(|| "installed package metadata is missing a required field".to_owned())
}

fn field_text<'a>(value: &'a serde_json::Value, name: &str) -> Result<&'a str, String> {
    field(value, name)?
        .as_str()
        .ok_or_else(|| "installed package metadata has an invalid text field".to_owned())
}

fn field_u64(value: &serde_json::Value, name: &str) -> Result<u64, String> {
    field(value, name)?
        .as_u64()
        .ok_or_else(|| "installed package metadata has an invalid integer field".to_owned())
}

fn field_hash(value: &serde_json::Value, name: &str) -> Result<String, String> {
    let hash = field_text(value, name)?;
    if !valid_hash(hash) {
        return Err("installed package metadata has an invalid SHA-256 field".to_owned());
    }
    Ok(hash.to_owned())
}

fn require_text(value: &serde_json::Value, name: &str, expected: &str) -> Result<(), String> {
    if field_text(value, name)? != expected {
        return Err(
            "installed package metadata does not match the requested coordinate".to_owned(),
        );
    }
    Ok(())
}

fn require_u64(value: &serde_json::Value, name: &str, expected: u64) -> Result<(), String> {
    if field_u64(value, name)? != expected {
        return Err("installed package metadata has an unsupported schema version".to_owned());
    }
    Ok(())
}

fn valid_hash(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn safe_coordinate(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
}

fn safe_leaf(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 255
        && value != "."
        && value != ".."
        && !value.contains('/')
        && !value.contains('\\')
        && !value.contains(':')
        && !value.contains('\0')
}

fn safe_relative_path(value: &str) -> bool {
    if value.is_empty() || value.len() > 4096 || value.starts_with('/') || value.contains('\\') {
        return false;
    }
    let path = Path::new(value);
    if path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return false;
    }
    value
        .split('/')
        .all(|part| !part.is_empty() && part != "." && part != ".." && !part.contains(':'))
}

fn validate_version(value: &str) -> Result<(), String> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b".+-_".contains(&byte))
    {
        return Err("dependency closure has an invalid tool version".to_owned());
    }
    Ok(())
}

fn is_reparse_point(metadata: &fs::Metadata) -> bool {
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0400;
        metadata.file_type().is_symlink()
            || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
    }
    #[cfg(not(windows))]
    {
        metadata.file_type().is_symlink()
    }
}
