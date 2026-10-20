# Rust Muse adapter — retirement status

`muse-rust-controller.1@0.1.2` is disabled for new work and is now a finite
retirement target. The retained Muse executor is the Node bridge documented in
[`modules/muse/README.md`](../../../modules/muse/README.md).

## Why it is not retained

The Rust candidate independently implements:

- MSP wire/schema and RPC transport;
- session fold and native command routing;
- a 226-KB journal;
- process custody;
- observations, streams, usage and result extraction;
- repeated Eliot bootstrap/descriptor/schema negotiation.

The main source files exceed 605 KB before several observation/result/owner modules.
The recorded 2026-10-17 receipt proves one bounded 0.1.2 dispatch/result path on its
recorded Core. It does not qualify the complete current route, process-family
recovery, all methods or installed fleet.

Keeping both Node and Rust would preserve two executors, two private journals, two
recovery implementations and two result normalizers.

## Existing data

Node and Rust private journals are intentionally not interchangeable.
`normalized_result::same_module` binds historical dispatch/result provenance to the
originating artifact.

For each concrete unsettled Rust Operation:

1. finish or reconcile with the current Rust owner;
2. export required result bytes;
3. otherwise record an explicit unresolved disposition;
4. never repeat native input to manufacture completion.

Completed artifacts remain readable through Store/artifact history. A private journal
name is not a user-data compatibility requirement.

## Deletion slice

After the inventory above:

- remove `crates/swarm-adapter-muse` from the Cargo workspace;
- remove the disabled descriptor, route/config examples and package selectors;
- remove custom MSP schema/wire copies and Rust-only fixtures;
- remove Rust Muse artifact allowlists and duplicated usage/result normalizers;
- remove installation/qualification branches that only select this candidate;
- update current documentation/status.

Do not add a Node↔Rust journal converter or retain the executable for hypothetical
history.

## Official Rust SDK

The official `muse-code-sdk` Rust crate is a separate future candidate. At reviewed
version 1.4.11 it supplies typed MSP, sessions, turns, approvals, fold, reconnect and
bounded close, but the high-level turn API mints command IDs internally.

Eliot requires the Operation→native-command association before uncertain submission.
Do not adopt the SDK as a third route until that production seam is available or a
small low-level adapter can preserve it while deleting more owned code than it adds.
Exact SDK/host version pairing also requires a separate live-qualified upgrade.

## Verification boundary

Before deletion, verify the concrete Rust-operation inventory and export. After
deletion, verify:

- no new route/descriptor/package selects the Rust artifact;
- no Cargo member or runtime caller remains;
- historical Store/artifact reads still work;
- Node bridge behavior is unchanged;
- no compatibility fallback silently launches Rust.