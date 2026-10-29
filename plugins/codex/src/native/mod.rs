mod client;
pub(crate) mod requests;
pub(crate) mod store_policy;
pub(crate) mod types;

pub(crate) use client::{NativeClient, NativeFrame, NativeLaunch, NativeProcess, StreamState};

pub(crate) fn native_no_write(code: &str) -> bool {
    matches!(code, "native_not_dispatched" | "busy" | "frame_too_large")
}
