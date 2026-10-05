//! Small OS process ownership and protected-file primitives.
//!
//! This crate does not launch providers, own Tasks, persist controller state,
//! or claim to sandbox same-user processes. Callers retain their own admission
//! and recovery policy and persist the returned owner identity as-is.

pub mod module_membership;
mod permissions;
pub mod process_group;

pub use module_membership::module_child_belongs_to_owner;
pub use permissions::{private_permissions, write_private_new};
pub use process_group::{
    Group, departed_empty, process_birth_identity, process_image_identity, spawned_departed,
    spawned_identity,
};
