//! Muse Rust transport foundation, not a usable runtime adapter.
//!
//! Dependency decision (2026-10-16): adopt whole `jsonrpsee-core = 0.26.1`
//! (MIT), async client AND public server router, with the already locked
//! `tokio-util = 0.7.19` (MIT) bounded line codec. Reviewed jsonrpsee source:
//! `bcc84901f3c882f4233f293e5acf018643cbd412`; Tokio tag `tokio-util-0.7.19`.
//! This replaces neither library with copied snippets. The official Muse SDK
//! remains the public wire/conformance reference, not a falsely claimed Rust
//! dependency: SDK 1.3.0 at `a7c10c5dd3f66be412077d29f9d11111af70317b`.
//! Its public 1.4.10 tree at `ae469342dfe6ccd5142947af5f7dcdd5d50ccc18`
//! still contains no Rust client. No owned Node transport is used here.
//!
//! Documentation Program section 0 permits complete crates at a named boundary;
//! its implementation review section 6 leaves checkpoint/Operation mapping and
//! explicit reconciliation with ELIOT. Architecture section 1 and modularity M4
//! require owned Rust control. Root must update the donor registry's recorded
//! transport boundary when integrating this crate; this is not "whole official
//! Muse SDK adoption". No Session/PendingCommandSet/GapFiller is copied.
//!
//! There is no established `docs/decisions` directory in the reviewed baseline,
//! so this scoped dependency decision stays with the owned library rather than
//! creating a new documentation convention.
//!
//! Production attachment verifies the existing module owner membership. The
//! caller supplies pipes from its owned child; this crate does not spawn, kill,
//! adopt, or discharge any process/Job custody. EOF is only a transport fact.
//! Explicit close drains locally admitted submissions before closing stdin;
//! neither close nor a written frame establishes native command completion.
//! Cancellation of a submission receipt does not cancel an admitted frame.
//!
//! Remaining work includes the complete command/TaskPrompt/model/readback facade,
//! private checkpoint and host delivery, children, results, recovery, streams,
//! packaging and native qualification. Native MCP inventory remains unavailable;
//! this library makes no dispatch-readiness claim. jsonrpsee payload trace
//! targets must remain disabled by the eventual worker's logging configuration.
//! Tests in `msp` are boundary fixtures written for Root to run, not qualification
//! of the installed Muse executable or any account/authentication state.

pub mod journal;
pub mod msp;
pub mod results;
pub mod schema;
