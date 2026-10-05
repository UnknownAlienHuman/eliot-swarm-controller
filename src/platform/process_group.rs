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
