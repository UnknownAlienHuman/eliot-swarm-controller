# Documentation

Everything needed to build Eliot v2: one current specification per topic, one roadmap
and compact lessons/donor/research references. Status lives only in roadmap checkboxes.

| Read | When |
| --- | --- |
| [vision](vision.md) | Product purpose, owner's source statement and R1–R11. |
| [architecture](architecture.md) | Components, donor-backed identity/event/recovery boundaries and data flows. |
| [roadmap](roadmap.md) | Connected layers, code budgets, implementation gate and final qualification. **Start here to pick work.** |
| [decisions](decisions.md) | Non-obvious current choices; no addenda. |
| [spec/core](spec/core.md) | Stable Agent + generation fence, events/source owners, verbs, commands and Store projections. |
| [spec/api](spec/api.md) | Local principals, API/CLI, exact wait/readback/drain semantics. |
| [spec/plugin-protocol](spec/plugin-protocol.md) | Plugin framing, source plan/cursors, generation binding, custody and recordings. |
| [spec/monitoring](spec/monitoring.md) | Source health, process custody, workspaces, Git, liveness and reconciliation. |
| [spec/automation](spec/automation.md) | Rule cursor/handoff, timers, actions-as-commands, scripts, approvals and Goals. |
| [spec/coordination](spec/coordination.md) | Authority, delivery identity/mail ack, vars, `who`, claims and hierarchy. |
| [spec/configuration](spec/configuration.md) | Private data root, frozen profiles/files, environment, accounts, plugins and services. |
| [harnesses/](harnesses/README.md) | Required native identity/process/source/cursor contract per harness. |
| [reference/lessons-v1](reference/lessons-v1.md) | Confirmed failure → compact prevention rule. |
| [reference/donors](reference/donors.md) | Exact donor source path and mechanism to copy; rejected application machinery. |
| [reference/research](reference/research.md) | Findings that constrain communication/automation. |

Edit the page that owns a topic in the same PR as the code that needs it. Replace
superseded text in place. Do not add report, audit, packet, inventory, plan or status
pages.

Implementation PRs use formatting plus smallest changed-package Clippy. The complete
runtime/conformance/live/platform/load suite is built and run after the connected local
product exists; docs or Clippy are never reported as runtime qualification.

v1 documentation remains at frozen commit `dc6f70aa` and, after L0.1, tag `v1-final`.
`docs/owner-decisions.md` remains only while v1 code embeds it; L0.1 removes both.
