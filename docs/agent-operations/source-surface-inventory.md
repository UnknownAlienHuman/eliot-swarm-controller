# Entrypoint and public-surface source inventory

Inventory branch head: `6e0d8d46cca7845bfa1c86e7375299f1b92aabfb`.
Live `origin/main` basis: `797ea93f1cfcaad8c8dde2629d666e64efb131c2`.
Combined inspected-cluster source SHA-256: `c55118a3580219f5fd1f4a90082eb51da3bc718e9d8516fac648007859d3d9f7`.

This file records syntax-level facts only. It does not prove authorization, runtime behavior, absence of hidden consumers or qualification.

## Cluster size and package consumers

| Cluster | Rust files | Lines | Bytes | Cargo dependents |
| --- | ---: | ---: | ---: | --- |
| `root-entrypoints` | 2 | 184 | 5608 | none |
| `swarm-cli` | 2 | 2464 | 82415 | none |
| `swarm-kernel` | 5 | 2739 | 96412 | `swarm-kernel-host` |
| `swarm-bus` | 4 | 1475 | 49273 | `swarm-kernel-host`, `swarm-scripts` |
| `swarm-telemetry` | 1 | 1719 | 61483 | `swarm-kernel-host`, `swarm-observer` |

## Entrypoint facts

- Root library re-exports `swarm-kernel-host`: `True`.
- Root binary locates a sibling `swarm-kernel-host`: `True`.
- Public CLI has explicit host delegation: `True`.
- Public CLI has explicit MCP delegation: `True`.
- Parsed top-level CLI command variants: 33.

### CLI command variants

`Host`, `Mcp`, `Gateway`, `ModuleRun`, `CheckWorker`, `OwnedOpencodeService`, `ScriptWorker`, `Source`, `Code`, `Check`, `Status`, `Doctor`, `Call`, `Task`, `Coordination`, `Concilium`, `Review`, `Automation`, `Script`, `Hook`, `Goal`, `GitHub`, `Launcher`, `ClientCreate`, `GmHandover`, `Agent`, `Attempt`, `Family`, `Result`, `Artifact`, `Report`, `Observer`, `Monitor`

## Exact Rust token consumers

### `swarm_kernel::`

- `crates/swarm-kernel/tests/kernel_failure.rs`
- `crates/swarm-kernel-host/src/acceptance.rs`
- `crates/swarm-kernel-host/src/automation/authorization.rs`
- `crates/swarm-kernel-host/src/host.rs`
- `crates/swarm-kernel-host/src/model.rs`
- `crates/swarm-kernel-host/src/review.rs`
- `crates/swarm-kernel-host/src/store/acceptance.rs`
- `crates/swarm-kernel-host/src/store/automation_acceptance.rs`
- `crates/swarm-kernel-host/src/store/automation_disposition.rs`
- `crates/swarm-kernel-host/src/store/mod.rs`
- `crates/swarm-kernel-host/src/store/operations.rs`
- `crates/swarm-kernel-host/src/store/review_disposition.rs`
- `crates/swarm-kernel-host/src/store/reviews.rs`
- `crates/swarm-kernel-host/src/store/status_reader.rs`
- `crates/swarm-kernel-host/src/store/submissions.rs`
- `crates/swarm-kernel-host/src/store/task_prompt.rs`
- `crates/swarm-kernel-host/src/store/tasks.rs`

### `swarm_bus::`

- `crates/swarm-bus/src/bin/dispatcher.rs`
- `crates/swarm-kernel-host/src/host_bus_supervisor.rs`
- `crates/swarm-kernel-host/src/store/automation_dispatch_bus.rs`
- `crates/swarm-scripts/src/error.rs`
- `crates/swarm-scripts/src/lib.rs`

### `swarm_telemetry::`

- `crates/swarm-kernel-host/src/host.rs`
- `crates/swarm-kernel-host/src/store/core_failure_tests.rs`
- `crates/swarm-kernel-host/src/store/logging.rs`
- `crates/swarm-kernel-host/src/store/mod.rs`
- `crates/swarm-kernel-host/src/store/module_supervisor_observation.rs`
- `crates/swarm-observer/src/host.rs`
- `crates/swarm-observer/src/lib.rs`
- `crates/swarm-observer/src/live_config.rs`

## File inventory

| Cluster | File | Lines | Public items | Markers |
| --- | --- | ---: | ---: | --- |
| `root-entrypoints` | `src/lib.rs` | 7 | 1 | compatibility=1, historical=1 |
| `root-entrypoints` | `src/main.rs` | 177 | 0 | wrapper=8 |
| `swarm-cli` | `crates/swarm-cli/src/lib.rs` | 277 | 5 | standalone=1 |
| `swarm-cli` | `crates/swarm-cli/src/main.rs` | 2187 | 0 | historical=1, delegate=5, wrapper=8 |
| `swarm-kernel` | `crates/swarm-kernel/src/acceptance.rs` | 745 | 26 | legacy=3 |
| `swarm-kernel` | `crates/swarm-kernel/src/dispatch.rs` | 123 | 7 | none |
| `swarm-kernel` | `crates/swarm-kernel/src/lib.rs` | 805 | 34 | none |
| `swarm-kernel` | `crates/swarm-kernel/src/reviews.rs` | 641 | 19 | historical=3 |
| `swarm-kernel` | `crates/swarm-kernel/src/tasks.rs` | 425 | 14 | none |
| `swarm-bus` | `crates/swarm-bus/src/bin/dispatcher.rs` | 872 | 0 | standalone=1 |
| `swarm-bus` | `crates/swarm-bus/src/lib.rs` | 349 | 35 | none |
| `swarm-bus` | `crates/swarm-bus/src/managed_worker_config.rs` | 211 | 2 | none |
| `swarm-bus` | `crates/swarm-bus/src/service_scope.rs` | 43 | 3 | none |
| `swarm-telemetry` | `crates/swarm-telemetry/src/lib.rs` | 1719 | 60 | compatibility=2 |

Machine-readable inventory: [`source-surface-inventory.json`](source-surface-inventory.json).
