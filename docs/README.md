# Documentation

**Целевая архитектура (v2.6, 27.11.2026) — [swarm/](swarm/INDEX.md).** Начинать с [swarm/ARCHITECTURE.md](swarm/ARCHITECTURE.md), затем рецепт контекста в [swarm/INDEX.md](swarm/INDEX.md) и следующий пункт [swarm/backlog.md](swarm/backlog.md). Где документы ниже расходятся с `swarm/`, права `swarm/`.

Документы ниже описывают текущий код до миграции.

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
| [Fluent desktop](design/fluent-design-system.md) | Current native Windows UI, interaction and accessibility contract; implementation stages stay in D12 |
| [cloud MCP](spec/cloud-mcp.md) | Local HTTP MCP child and dedicated Cloudflare Tunnel lifecycle |
| [harnesses](harnesses/README.md) | Native capability notes; Codex first |
| [donors](reference/donors.md) | Dependency/extract/pattern map with source and target paths |

Core/bus, direct Codex/Luna and the subsequent automation/adapter layers are implemented;
the roadmap owns remaining implementation and qualification. Material is frozen until
Reactor/WinUI fully support it and the Owner reopens that direction. A dependency update
does not change the current Fluent contract. Update the page that owns a changed contract; do not create
reports, addenda, proof packets or another status registry.

v1 code and its original notices are preserved on branch `archive/v1-final`, at commit
`dc6f70aa08ea5fad3da04624ce50c46d45edd57c`. Read pinned donor files with
`git show dc6f70aa:<path>`; the old owner-decisions and runtime are not active v2 inputs.
