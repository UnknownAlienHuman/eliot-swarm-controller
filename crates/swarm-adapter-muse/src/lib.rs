//! Muse Rust transport and gated durable adapter source.
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
//! Production transport attachment verifies existing module-owner membership.
//! The native_owner module retains the real spawned child in an external joined
//! custodian; transport attachment itself neither owns nor discharges that
//! process/Job custody. No kill or adoption is implemented. EOF is a transport fact.
//! Explicit close drains locally admitted submissions before closing stdin;
//! neither close nor a written frame establishes native command completion.
//! Cancellation of a submission receipt does not cancel an admitted frame.
//!
//! The command/TaskPrompt/model facade, journal, Host delivery, child observations,
//! results, explicit recovery and streams are integrated source components. The
//! executable uses the joined runtime; its descriptor remains disabled pending
//! platform/native qualification; see registration/INTEGRATION.md. MCP inventory remains
//! unavailable. Keep jsonrpsee payload trace targets disabled in worker logging.
//! Tests in `msp` are boundary fixtures written for Root to run, not qualification
//! of the installed Muse executable or any account/authentication state.

pub mod commands;
pub mod driver;
pub mod journal;
pub mod module_runtime;
pub mod msp;
pub mod native_owner;
pub(crate) mod observations;
pub mod results;
pub mod schema;
pub mod streams;
