//! Owner-only file editing for global and registered-project rules.
//!
//! Saves validate a candidate against the complete namespace with the shared
//! reload parser, then atomically publish the exact validated bytes. The
//! existing filesystem watcher and periodic reconciliation apply the change.
use std::{fs, path::Path};

use eliot_core::{
    Error, Result, SecretPolicy,
    api::{
        MAX_RULE_FILE_BYTES, REDACTION_MARKER, RuleDeleteRequest, RuleDeleteResponse,
        RuleReadRequest, RuleReadResponse, RuleSaveRequest, RuleSaveResponse,
    },
};
use serde_json::Value;

use crate::platform::script_file::{self, Directory};

use super::{projects, reload};

struct ApprovedRoot {
    registration: Option<projects::RegistrationSnapshot>,
}

async fn approved_root(data_root: &Path, project: Option<&str>) -> Result<ApprovedRoot> {
    match project {
        None => Ok(ApprovedRoot { registration: None }),
        Some(id) => {
            let registration = projects::rule_edit_registration(data_root, id).await?;
            Ok(ApprovedRoot {
                registration: Some(registration),
            })
        }
    }
}

fn file_guard() -> std::sync::MutexGuard<'static, ()> {
    reload::RULE_FILE_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn open_child(parent: &Directory, name: &str, create: bool) -> Result<Directory> {
    if !create {
        return Directory::open(&parent.path.join(name));
    }
    match parent.create_directory(name) {
        Ok(directory) => Ok(directory),
        Err(create_error) => Directory::open(&parent.path.join(name)).map_err(|_| create_error),
    }
}

fn open_root(data_root: &Path, root: &ApprovedRoot, create: bool) -> Result<Directory> {
    if let Some(registration) = &root.registration {
        let checkout = Directory::open(Path::new(&registration.checkout))?;
        let metadata = open_child(&checkout, ".eliot", create)?;
        open_child(&metadata, "rules", create)
    } else {
        let data = Directory::open(data_root)?;
        open_child(&data, "rules", create)
    }
}

fn existing_file(directory: &Directory, path: &str) -> Result<(std::fs::File, Vec<u8>)> {
    let full_path = directory.path.join(path);
    let metadata = fs::symlink_metadata(&full_path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            Error::new("not_found", "rule file does not exist")
        } else {
            Error::new("unavailable", "rule file cannot be inspected")
        }
    })?;
    if metadata.file_type().is_symlink()
        || crate::platform::is_reparse_point(&metadata)
        || !metadata.is_file()
    {
        return Err(Error::new("not_allowed", "rule path is not a regular file"));
    }
    let mut file = directory.file(path)?;
    let bytes = script_file::read(&mut file)?;
    if bytes.len() > MAX_RULE_FILE_BYTES {
        return Err(Error::new("invalid", "rule file exceeds the 128 KiB bound"));
    }
    std::str::from_utf8(&bytes)
        .map_err(|_| Error::new("invalid", "rule file is not valid UTF-8"))?;
    Ok((file, bytes))
}

fn file_exists(directory: &Directory, path: &str) -> Result<bool> {
    match fs::symlink_metadata(directory.path.join(path)) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink()
                || crate::platform::is_reparse_point(&metadata)
                || !metadata.is_file()
            {
                return Err(Error::new("not_allowed", "rule path is not a regular file"));
            }
            Ok(true)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(_) => Err(Error::new("unavailable", "rule path cannot be inspected")),
    }
}

fn known_secret(policy: &SecretPolicy, text: &str) -> bool {
    if text.contains(REDACTION_MARKER) || policy.contains_value(&Value::String(text.to_owned())) {
        return true;
    }
    toml::from_str::<toml::Value>(text)
        .ok()
        .and_then(|parsed| serde_json::to_value(parsed).ok())
        .is_some_and(|parsed| policy.contains_value(&parsed))
}

fn reject_secret_source(policy: &SecretPolicy, source: &str) -> Result<()> {
    if known_secret(policy, source) {
        Err(Error::new(
            "secret_in_payload",
            "rule file contains a known secret value or redaction marker",
        ))
    } else {
        Ok(())
    }
}

fn reject_secret_namespace(policy: &SecretPolicy, snapshot: &reload::RuleSnapshot) -> Result<()> {
    for definition in &snapshot.definitions {
        let value = serde_json::to_value(definition)
            .map_err(|_| Error::new("invalid", "rule definition cannot be checked"))?;
        if policy.contains_value(&value) {
            return Err(Error::new(
                "secret_in_payload",
                "rule namespace contains a known secret value",
            ));
        }
    }
    Ok(())
}

fn response_is_safe<T: serde::Serialize>(policy: &SecretPolicy, response: &T) -> bool {
    serde_json::to_value(response).is_ok_and(|value| !policy.contains_value(&value))
}

fn revision(bytes: &[u8]) -> String {
    eliot_core::api::inference::sha256_hex(bytes)
}

pub(crate) async fn read(
    data_root: &Path,
    request: &RuleReadRequest,
    policy: &SecretPolicy,
) -> Result<RuleReadResponse> {
    request.validate()?;
    if known_secret(policy, request.path.as_str())
        || request
            .project
            .as_deref()
            .is_some_and(|project| known_secret(policy, project))
    {
        return Err(Error::new(
            "not_allowed",
            "rule file identity contains a known secret",
        ));
    }
    let root = approved_root(data_root, request.project.as_deref()).await?;
    let _guard = file_guard();
    let directory = open_root(data_root, &root, false).map_err(|error| {
        if error.code == "not_allowed" {
            Error::new("not_found", "rule root is not available")
        } else {
            error
        }
    })?;
    let (_, bytes) = existing_file(&directory, &request.path)?;
    let content = String::from_utf8(bytes.clone())
        .map_err(|_| Error::new("invalid", "rule file is not valid UTF-8"))?;
    reject_secret_source(policy, &content)?;
    let response = RuleReadResponse {
        project: request.project.clone(),
        path: request.path.clone(),
        revision: revision(&bytes),
        size: bytes.len() as u64,
        content,
    };
    if !response_is_safe(policy, &response) {
        return Err(Error::new(
            "not_allowed",
            "rule response contains a known secret",
        ));
    }
    request.validate_response(&response)?;
    Ok(response)
}

pub(crate) async fn save(
    data_root: &Path,
    request: &RuleSaveRequest,
    policy: &SecretPolicy,
) -> Result<RuleSaveResponse> {
    request.validate()?;
    if known_secret(policy, &request.path)
        || request
            .project
            .as_deref()
            .is_some_and(|project| known_secret(policy, project))
    {
        return Err(Error::new(
            "not_allowed",
            "rule file identity contains a known secret",
        ));
    }
    reject_secret_source(policy, &request.content)?;
    let root = approved_root(data_root, request.project.as_deref()).await?;
    let _guard = file_guard();
    let directory = open_root(data_root, &root, request.expected_revision.is_none())?;

    let existing = if let Some(expected) = &request.expected_revision {
        let (file, bytes) = existing_file(&directory, &request.path).map_err(|error| {
            if error.code == "not_found" {
                Error::new(
                    "conflict",
                    "rule file no longer matches its expected revision",
                )
            } else {
                error
            }
        })?;
        if revision(&bytes) != *expected {
            return Err(Error::new(
                "conflict",
                "rule file changed since it was read; read it again and reapply the edit",
            ));
        }
        reject_secret_source(
            policy,
            std::str::from_utf8(&bytes)
                .map_err(|_| Error::new("invalid", "rule file is not valid UTF-8"))?,
        )?;
        Some((file, bytes))
    } else {
        if file_exists(&directory, &request.path)? {
            return Err(Error::new(
                "conflict",
                "rule file already exists; read it before replacing",
            ));
        }
        None
    };

    let filename = if existing.is_some() && cfg!(windows) {
        fs::canonicalize(directory.path.join(&request.path))
            .ok()
            .and_then(|file| file.file_name()?.to_str().map(str::to_owned))
            .filter(|name| name.ends_with(".toml"))
            .ok_or_else(|| Error::new("conflict", "rule file name could not be verified"))?
    } else {
        request.path.clone()
    };
    let candidate = match &root.registration {
        None => reload::load_with_file_override(data_root, &filename, request.content.as_bytes())?,
        Some(registration) => {
            let snapshot = reload::load_project_with_file_override(
                Path::new(&registration.checkout),
                &filename,
                request.content.as_bytes(),
            )?;
            projects::validate_project_rule_actions(&snapshot.inputs, registration)?;
            snapshot
        }
    };
    reject_secret_namespace(policy, &candidate)?;

    let response_revision = if let Some((file, bytes)) = existing {
        let content = request.content.as_bytes();
        if bytes != content {
            directory.replace(&filename, &file, &bytes, content)?;
        }
        revision(content)
    } else {
        directory.publish_new(&request.path, request.content.as_bytes())?;
        revision(request.content.as_bytes())
    };
    let response = RuleSaveResponse {
        project: request.project.clone(),
        path: request.path.clone(),
        saved: true,
        revision: response_revision,
    };
    if !response_is_safe(policy, &response) {
        return Err(Error::new(
            "unknown",
            "rule file was saved but its response could not be checked",
        ));
    }
    request
        .validate_response(&response)
        .map_err(|error| Error::new("unknown", error.message.to_string()))?;
    Ok(response)
}

pub(crate) async fn delete(
    data_root: &Path,
    request: &RuleDeleteRequest,
    policy: &SecretPolicy,
) -> Result<RuleDeleteResponse> {
    request.validate()?;
    if known_secret(policy, &request.path)
        || request
            .project
            .as_deref()
            .is_some_and(|project| known_secret(policy, project))
    {
        return Err(Error::new(
            "not_allowed",
            "rule file identity contains a known secret",
        ));
    }
    let root = approved_root(data_root, request.project.as_deref()).await?;
    let _guard = file_guard();
    let directory = open_root(data_root, &root, false)?;
    let (_, bytes) = existing_file(&directory, &request.path).map_err(|error| {
        if error.code == "not_found" {
            Error::new(
                "conflict",
                "rule file no longer matches its expected revision",
            )
        } else {
            error
        }
    })?;
    if revision(&bytes) != request.expected_revision {
        return Err(Error::new(
            "conflict",
            "rule file changed since it was read; read it again before deleting",
        ));
    }
    directory.remove_file(&request.path, &bytes)?;

    let response = RuleDeleteResponse {
        project: request.project.clone(),
        path: request.path.clone(),
        deleted: true,
    };
    if !response_is_safe(policy, &response) {
        return Err(Error::new(
            "unknown",
            "rule file was deleted but its response could not be checked",
        ));
    }
    request
        .validate_response(&response)
        .map_err(|error| Error::new("unknown", error.message.to_string()))?;
    Ok(response)
}
