pub(crate) mod client;
pub(crate) mod server;
pub(crate) mod sse;
pub(crate) mod types;

pub(crate) use client::{HttpOutcome, OpenCodeHttp};
pub(crate) use server::{OwnedServer, ServerLaunch};

/// Local non-dispatch codes: the write provably never reached the server,
/// so the host may report `rejected` (never sent, safe to supersede).
pub(crate) fn native_no_write(code: &str) -> bool {
    matches!(code, "native_not_dispatched" | "busy" | "frame_too_large")
}
