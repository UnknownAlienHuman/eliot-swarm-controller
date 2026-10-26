# Documentation

Everything needed to build Eliot v2: one current specification per topic, an
implementation-first roadmap and three compact reference pages. Status lives only in
roadmap checkboxes.

| Read | When |
| --- | --- |
| [vision](vision.md) | Product purpose, owner's original statement and requirements R1–R11. |
| [architecture](architecture.md) | Components, authority boundaries, command/event flows and scale model. |
| [roadmap](roadmap.md) | Work order, scoped implementation gate, code budgets and the deferred L7 qualification campaign. **Start here to pick work.** |
| [decisions](decisions.md) | Non-obvious design choices and defaults. |
| [spec/core](spec/core.md) | Logical identity/generation, states, events, verbs, commands and Store schema. |
| [spec/api](spec/api.md) | Local IPC, daemon API, drain semantics and CLI. |
| [spec/plugin-protocol](spec/plugin-protocol.md) | Per-agent/shared plugin modes, exact native binding, custody, recordings and conformance contract. |
| [spec/monitoring](spec/monitoring.md) | Source ownership, process custody, Git/filesystem facts, liveness and views. |
| [spec/automation](spec/automation.md) | Rules, durable run/action journal, timers, scripts, approvals and Goal loop. |
| [spec/coordination](spec/coordination.md) | Control, mailbox, shared vars, `who`, claims and hierarchy. |
| [spec/configuration](spec/configuration.md) | Data directory, profiles, plugin modes, proxies and services. |
| [harnesses/](harnesses/README.md) | Vendor capability matrix and one sourced page per supported harness. |
| [reference/lessons-v1](reference/lessons-v1.md) | Confirmed v1 failure → v2 prevention rule. |
| [reference/donors](reference/donors.md) | Narrow mechanisms/code to borrow and anti-donors to reject. |
| [reference/research](reference/research.md) | Research findings that materially constrain coordination/automation. |

Edit the page that owns the topic in the same PR as the code that needs it. Replace
superseded text in place. Do not add report, audit, packet, inventory, plan or status
pages.

Implementation PRs run formatting and the smallest changed-package Clippy gate.
Broad deterministic, platform, crash/restart, live-harness and load tests are built and
run together in roadmap L7 after the local feature set is complete; source review or
Clippy is never reported as runtime qualification.

v1 documentation remains in Git history and, after L0.1, at tag `v1-final`:
`git show v1-final:docs/<path>`. Before the tag is created use commit `dc6f70aa`.
`docs/owner-decisions.md` remains only while v1 code embeds it; L0.1 deletes both.
