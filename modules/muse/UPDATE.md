# Muse bridge — update and transition

The current default Muse route uses this Node bridge. The
[Rust adapter](../../crates/swarm-adapter-muse/registration/INTEGRATION.md) is an
integrated candidate with a disabled descriptor. Refactoring selects one
executor and removes the replaced implementation under the
[central retirement plan](../../docs/agent-operations/implementation.md#current-format-and-existing-data).

## Current source facts

| Component | Recorded source |
| --- | --- |
| Node bridge | `muse-sdk-1.3.0-bridge.13`; ELIOT-owned `*.mjs` in this directory. |
| SDK resolution | `@muse-code/sdk` 1.3.0 in `package.json` and `package-lock.json`; Node requirement is `>=22`. |
| Inspected wire source | [Muse SDK commit a7c10c5d](https://github.com/meta-models/muse-code-sdk/tree/a7c10c5dd3f66be412077d29f9d11111af70317b); source provenance is separate from the installed native version. |
| Native harness | Existing Muse executable from private configuration; Muse owns provider authorization, settings and inference. |
| Rust candidate | `muse-rust-controller.1@0.1.2`; its journal and current result checks differ from the Node path. |

These values describe the inspected source. Maintain dependencies through their
normal manifest and lockfile; resolving a new SDK version does not by itself
create a new protocol, artifact namespace or permanent runtime generation.
Change a schema version when the exchanged data or its meaning is incompatible.
Record the shipped build separately from that protocol decision.

## Update the selected executor

Keep provider control in the native harness and transport mechanics in the
chosen SDK/library. Change the actual caller and remove replaced glue together.
Do not patch installed `node_modules`; update the dependency or its narrow
adapter boundary. Dependency installation remains a development/package step.

For changed bridge behavior, use the existing syntax/import and
`selftest.mjs` checks relevant to that boundary. The fixtures cover checkpoint
reopen, input correlation, bounded observation and native-request handling.
They do not establish installed provider availability; record only checks that
actually ran under the [scoped verification rules](../../docs/ci-scoped-builds.md).

## Current transition gaps

The bridge currently requires `moduleArtifactId` to equal bridge.13 and checks
the saved checkpoint against the credential and artifact. The Host also
recognizes earlier bridge identities in its native-usage consumer. The Rust
candidate reads its own scope marker/journal and cannot adopt a Node checkpoint.
These are existing implementation constraints to remove through a finite
transition, not requirements to retain every bridge release.

Choose one executor for new work after comparing required consumers and total
owned code across Rust, SDK drivers and glue. For actual work still using the
replaced executor, complete or explicitly reconcile the admitted native
operation, then export any result/history that must survive. Translate data only
where the current format can represent it; otherwise keep an ordinary archive
or create a new Task/Attempt with an explicit boundary. Remove the old runtime
readers, version branches and package checks when that transition is complete.
Do not relabel an old receipt or replay an uncertain prompt as a migration technique.

Rollback selects the previous release for subsequent work; it does not undo a
native effect. Handle already admitted work through the same finite transition
instead of committing to indefinite parallel bridge copies. The source change
that removes an executor also removes its obsolete setup and verification steps.
