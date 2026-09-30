# Muse native module — implementation checkpoint

The integration uses the complete published `@muse-code/sdk` 1.3.0 and reviewed source at `meta-models/muse-code-sdk@a7c10c5dd3f66be412077d29f9d11111af70317b`. No model loop, MSP codec, user login, or global Muse configuration is replaced.

## Recovery checkpoint — 2026-09-30

The interrupted continuation published only dependency/input preparation (`0e3ccd6b` and `db45be62`). Relative to the last built controller at `c37e6bbf`, no Rust source or runnable bridge was added. That controller's Windows/Linux formatting, Clippy and release builds passed in [run 36693929400](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/36693929400).

Recovered [input artifact](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/36696436986): `native-integration-inputs`, artifact ID `11087808310`, SHA-256 `b0e06843336290e685cd79376c709be4ac7ab79accf3cb09f9bd2beb1826ad52`. Its archive and CRC were checked. It contains upstream SDK source, schema/fixtures, published package, and the module lockfile, not an Eliot bridge. The recovered lockfile is now committed beside package.json, so package resolution is not dependent on retaining a temporary Actions artifact.

Earlier progress messages mentioned local dispatch/OpenCode work. Those implementation bytes were not found in the current mounted files, retrieved file results or published Git tree; do not treat those messages as recovered or qualified code. No rollback of the working controller is needed.

## Resume implementation

Start with the SDK-owned bridge and a real RuntimePort consumer: reserve Operation, commit before native admission, map native identity/outcome, and keep reader/replies independent of turn completion. Then connect the direct OpenCode V2 implementation to the same contract. Native family completeness and stream recovery must remain explicit, not inferred from the old progress messages.

A separate Node process owns the SDK's `muse serve` stdio; the Rust host connects over local IPC. Losing that connection must not call SDK.close or terminate Muse. A bridge crash is a different failure boundary and cannot promise uninterrupted native execution.

For dependency preparation, run `npm ci --ignore-scripts` in this directory when needed. It installs only the locked local dependency, not a vendor executable or a global package; it does not make this module runnable. Keep the donor fixtures/notices at the pinned source. The preparation workflow is temporary build tooling, not product infrastructure. No C03 completion, SDK/native execution or model-quota consumption is claimed by this checkpoint.
