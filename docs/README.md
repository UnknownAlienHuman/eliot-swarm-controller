# Documentation

Start with [vision](vision.md), [architecture](architecture.md) and the next task in
[roadmap](roadmap.md). Then read only its owning spec and donor entry. The final product
contract is not a requirement to implement every feature in the first layer.

| Page | Owns |
| --- | --- |
| [vision](vision.md) | Owner statement and product requirements |
| [architecture](architecture.md) | Modules, dependency direction and single command/event paths |
| [roadmap](roadmap.md) | Build order, current checkboxes and deferred qualification |
| [decisions](decisions.md) | Short rationale for choices, not another specification |
| [core](spec/core.md) | Identities, states, commands, events and Store projections |
| [API](spec/api.md) | IPC, caller identity, admission, wait/readback and CLI |
| [plugin protocol](spec/plugin-protocol.md) | Implemented capabilities, transport and native fact settlement |
| [monitoring](spec/monitoring.md) | Process/source/Git/workspace facts and liveness |
| [automation](spec/automation.md) | Rules, timers, scripts and Goals using normal commands |
| [coordination](spec/coordination.md) | Mail, variables, hierarchy, who and advisory claims |
| [configuration](spec/configuration.md) | Profiles, environment, routes and services |
| [harnesses](harnesses/README.md) | Native capability notes; Codex first |
| [donors](reference/donors.md) | Dependency/extract/pattern map with source and target paths |
| [v1 lessons](reference/lessons-v1.md) | Historical failure evidence, not additional implementation gates |
| [research](reference/research.md) | Background evidence, not a required reading queue |

The first usable milestone is direct Codex/Luna after core/bus. Automation, communication
and other harnesses follow. Update the page that owns a changed contract; do not create
reports, addenda, proof packets or another status registry. v1 remains at `dc6f70aa`
and, once created/verified, tag `v1-final`.
