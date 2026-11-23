//! Map Codex spawn items and child threads to the shared registration identity.

use eliot_core::ChildRegistration;
pub(crate) use eliot_plugin::native_child::{
    ChildBinding, binding_from_ack, is_admission_rejection, register_child,
};

/// Registration identity for one native child thread.
///
/// The spawn item is stable for one native spawn call, so an idempotent retry
/// returns the same registration id instead of a second child.
pub(crate) fn registration_id(spawn_item_id: &str, child_thread: &str) -> String {
    format!("codex:spawn:{spawn_item_id}:{child_thread}")
}

/// Build a registration for a confirmed child of `parent_native_ref`.
///
/// `parent_id` / `parent_generation` are the immediate parent Agent, never the
/// root when the parent itself is a child.
pub(crate) fn build_registration(
    spawn_item_id: &str,
    parent_id: &str,
    parent_generation: i64,
    parent_native_ref: &str,
    native_scope: &str,
    child_thread: &str,
) -> ChildRegistration {
    ChildRegistration {
        registration_id: registration_id(spawn_item_id, child_thread),
        parent_id: parent_id.to_owned(),
        parent_generation,
        parent_turn_id: None,
        parent_native_ref: parent_native_ref.to_owned(),
        native_scope: native_scope.to_owned(),
        native_ref: child_thread.to_owned(),
    }
}
