//! Host compatibility boundary for the independent OS ownership package.
use crate::error::Result;
use serde_json::Value;

pub struct Group(swarm_process::Group);

impl std::ops::Deref for Group {
    type Target = swarm_process::Group;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Group {
    pub fn enter(token: &str) -> Result<Self> {
        swarm_process::Group::enter(token)
            .map(Self)
            .map_err(Into::into)
    }

    pub fn enter_script(token: &str) -> Result<Self> {
        swarm_process::Group::enter_script(token)
            .map(Self)
            .map_err(Into::into)
    }

    pub fn enter_module(token: &str) -> Result<Self> {
        swarm_process::Group::enter_module(token)
            .map(Self)
            .map_err(Into::into)
    }

    #[cfg(windows)]
    pub fn open_check_query(identity: &Value, token: &str) -> Result<Self> {
        swarm_process::Group::open_check_query(identity, token)
            .map(Self)
            .map_err(Into::into)
    }

    pub fn children_empty(&self) -> Result<bool> {
        self.0.children_empty().map_err(Into::into)
    }

    pub fn cancel_children(&self) -> Result<u64> {
        self.0.cancel_children().map_err(Into::into)
    }

    pub fn disarm(&self) -> Result<()> {
        self.0.disarm().map_err(Into::into)
    }
}

pub fn departed_empty(identity: &Value, token: &str) -> Result<bool> {
    swarm_process::departed_empty(identity, token).map_err(Into::into)
}

pub fn spawned_identity(pid: u32) -> Result<Value> {
    swarm_process::spawned_identity(pid).map_err(Into::into)
}

pub fn spawned_departed(identity: &Value, token: &str) -> Result<bool> {
    swarm_process::spawned_departed(identity, token).map_err(Into::into)
}

pub fn process_birth_identity(pid: u32) -> Result<Option<Value>> {
    swarm_process::process_birth_identity(pid).map_err(Into::into)
}

pub fn process_image_identity(pid: u32) -> Result<Value> {
    swarm_process::process_image_identity(pid).map_err(Into::into)
}

#[cfg(test)]
pub(crate) fn classify_image_error(code: &str, message: &str) -> &'static str {
    // Exact literals from process_group.rs, never substrings or raw diagnostics.
    match (code, message) {
        ("PROCESS_IDENTITY", "process executable path is not valid Unicode") => {
            "image_path_not_unicode"
        }
        ("PROCESS_IDENTITY", "process executable path is outside the supported boundary") => {
            "image_path_boundary"
        }
        ("PROCESS_IDENTITY", "process executable is outside the supported file boundary") => {
            "image_file_boundary"
        }
        ("PROCESS_IDENTITY", "process image size overflow") => "image_size_overflow",
        ("PROCESS_IDENTITY", "process executable exceeds the supported hash boundary") => {
            "image_hash_boundary"
        }
        ("PROCESS_IDENTITY", "process executable changed during bounded hashing") => {
            "image_hash_changed"
        }
        ("PROCESS_IDENTITY", "invalid proc stat") => "proc_stat_invalid",
        ("PROCESS_IDENTITY", "incomplete proc stat") => "proc_stat_incomplete",
        ("PROCESS_IDENTITY", "invalid process group") => "proc_group_invalid",
        ("PROCESS_IDENTITY", "cannot observe process pidfd") => "pidfd_unobservable",
        ("PROCESS_IDENTITY", "kernel boot identity is unavailable") => "boot_id_unavailable",
        ("PROCESS_IDENTITY", "kernel executable path is not absolute") => "exe_path_not_absolute",
        // This source branch combines birth/liveness/path/device/inode checks;
        // its reason does not identify which individual condition failed.
        (
            "PROCESS_IDENTITY",
            "process birth or executable image changed during bounded hashing",
        ) => "post_hash_identity_changed",
        ("PROCESS_GONE", "process is no longer live") => "process_gone",
        ("PROCESS_GONE", "process executable disappeared") => "process_executable_gone",
        ("PROCESS_GONE", "process birth changed while opening its executable") => {
            "process_birth_changed_on_open"
        }
        ("PROCESS_GONE", "process exited during inventory") => "process_gone_during_inventory",
        _ => "unclassified",
    }
}
