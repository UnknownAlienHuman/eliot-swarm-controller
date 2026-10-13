# REG23-P2-08: atomic module demand and hard failure propagation

Module-demand projection previously converted every failure in five Store call sites into an ordinary blocked binding. A storage or shared-catalog error could therefore return a successful demand snapshot, and owner intents reserved earlier in the call remained committed.

The repair limits blocked results to explicit domain errors at each call site. Store, shared-catalog, conversion, commit, and unrecognized errors propagate to the caller. One Immediate transaction now covers owner intent reservation and the complete demand projection, so a late hard failure rolls back earlier reservations. Readback inside that transaction reuses its snapshot; the standalone readback wrapper retains its own transaction.

The private consumer fixture uses retained Task, Attempt, launch, agent.open, held lease, actor validation, and the final workspace manifest digest. It preserves authorization and nonce idempotency. SQL triggers fail the second real intent insertion or damage the shared catalog after a real reservation; the tests assert rollback of both the reservation and catalog mutation.

## Validation

Rust 1.98.1. On Windows and Linux, strict package Clippy passed for lib+tests and lib+bins with warnings denied. All 854 source inputs were hash-bound. The lib+tests result is from v2; production lib+bins is retained from v1 after checking every production input unchanged.

Seven unaffected demand consumers passed in v2. Five owned consumers passed after explicit pinned debug-source resource setup and a fresh host compilation in v4. No passing unaffected test was repeated. The v4 nextest IDs are:

- Windows: `4d07ab24-d7ac-4dfe-b55e-be10cdd44bad`.
- Linux: `b94bf061-3149-4284-b7af-914324e3f685`.

The five owned cases cover complete commit and idempotent nonces, second-insert failure, late shared-catalog damage, late readback Store failure, and a stale owned subject beside a healthy subject. The remaining seven cover semantic binding isolation, catalog Store failure, and the existing exact readback/custody cases.

Earlier fixture failures are retained as failed evidence: missing local Operator pin, missing explicit source resources, and a shared-target test executable whose compiled source root still pointed at the preceding copy. The last was resolved by forcing only host recompilation; source bytes, production checks, and assertions were unchanged. Dependency setup used the pinned package-lock with scripts disabled. These Store consumers do not execute OpenCode JavaScript, SDKs, native services, or models; the resource setup does not qualify a Linux native dependency closure.

## Source binding and remaining scope

Postimage SHA-256:

- `module_demand.rs`: `A2D607201F5F62306831A85347FB24C3D24299B510895658446996B27499D608`.
- `launcher.rs`: `09E49E4C10617C04F091C1D45E88879B0CE366BF4208D9849A0A589880020904`; only private test registration changes.
- `module_demand/reg23_hard_failure_tests.rs`: `D3C5B50B032B75FBD77DBD41287A266179CF3F497E0F5F2A0F9F3F4966D95059`.

This is the bounded REG23-P2-08 implementation and Store consumer evidence. Other REG-23 sites and all 459 original V27 obligations remain tracked. Full installed public/native/platform/model/crash/fault/load qualification remains required.
