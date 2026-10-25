# Documentation

Everything an agent needs to build Eliot v2. Nothing here is history or status
except the roadmap checkboxes.

| Read | When |
| --- | --- |
| [vision](vision.md) | What Eliot is; the owner's statement (source of truth); requirements R1–R11. |
| [architecture](architecture.md) | Components, data flows, scale model, repository layout. |
| [roadmap](roadmap.md) | Layers L0–L9, one task per PR, exit tests, code budgets. **Start here to pick work.** |
| [decisions](decisions.md) | Why things are the way they are; open questions with defaults. |
| [spec/core](spec/core.md) | Ids, agent model and states, events, verbs, commands, schema. |
| [spec/api](spec/api.md) | Daemon API and the CLI. |
| [spec/plugin-protocol](spec/plugin-protocol.md) | How harness plugins talk to the daemon; hooks; conformance kit. |
| [spec/monitoring](spec/monitoring.md) | Process, Git, transcript and liveness monitoring; views. |
| [spec/automation](spec/automation.md) | Rules, timers, actions, scripts, approvals, Goal loop, guardrails. |
| [spec/coordination](spec/coordination.md) | Permissions, mailbox, shared vars, `who`, claims, hierarchy. |
| [spec/configuration](spec/configuration.md) | Data dir, config files, profiles, proxies. |
| [harnesses/](harnesses/README.md) | Capability matrix and one page per vendor harness, with official sources. |
| [reference/lessons-v1](reference/lessons-v1.md) | What went wrong in v1 and the rule that prevents it. |
| [reference/donors](reference/donors.md) | Code and patterns to borrow, and what not to borrow. |
| [reference/research](reference/research.md) | Research findings behind the coordination and automation rules. |

How to change docs: edit the page that owns the topic in the same PR as the code
that needs it. Do not add new documents for reports, audits, plans or status.

v1 documentation (61k lines) was removed from the tree; read it with
`git show v1-final:docs/<path>` (until the tag exists: `git show dc6f70aa:docs/<path>`).
`docs/owner-decisions.md` is still present only because v1 code embeds it in a test;
it is not a requirement and is deleted with the v1 code in L0.1.
