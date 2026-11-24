//! Adapter declarations used by the daemon and the offline desktop editor.

pub(crate) use eliot_core::api::HarnessCapabilities;
pub(crate) use eliot_core::api::harness::ProfileCapabilities;
use eliot_core::api::native_profile::NativeSubagentsPolicy;

pub fn native_subagents_policy(harness: &str) -> Option<NativeSubagentsPolicy> {
    ProfileCapabilities::of(harness).native_subagents_policy
}
