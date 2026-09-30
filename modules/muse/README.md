# Muse native module — implementation checkpoint

The next integration uses the complete published `@muse-code/sdk` 1.3.0, corresponding to the reviewed source at `meta-models/muse-code-sdk@a7c10c5dd3f66be412077d29f9d11111af70317b`. No model loop, MSP codec, user login, or global Muse configuration is replaced.

This checkpoint pins the donor before implementing its adapter. It does not yet provide a runnable bridge or mark C03 complete. The preparation workflow only fetches source/package inputs and a formatter; it does not launch a native agent or spend model quota. It has read-only GitHub permissions and will be removed after the inputs are consumed.

Planned boundary: a separate Node process owns the SDK's `muse serve` stdio; the Rust host connects over local IPC. Losing that connection must not call SDK.close or terminate Muse. A bridge crash is a different failure boundary and cannot promise uninterrupted native execution.
