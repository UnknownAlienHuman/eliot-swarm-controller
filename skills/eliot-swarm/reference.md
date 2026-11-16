# eliot-swarm reference

Canonical method names and their CLI aliases. Confirm fields, examples and
limits with `api.help` (`swarm <cmd> --help`) before calling: this map
carries no schemas.

| Method | CLI | Notes |
| --- | --- | --- |
| `api.identity` | `swarm identity` | Your id, generation, allowed scopes. |
| `api.methods` | (MCP only) | List/search callable methods. |
| `api.help` | (MCP only) | Schemas, example, mutation-id rule. |
| `who` | `swarm who` | Workers plus advisory claims; own row marked. |
| `mail.send` | `swarm msg send` | Only `queue` mode is connected. |
| `mail.inbox` | `swarm inbox` | Page unread mail; reading never acks. |
| `mail.read` | `swarm msg show` | Read one delivery without acking. |
| `mail.ack` | `swarm msg ack` | Ack exact delivery ids consumed. |
| `var.set` | `swarm var set` | CAS via `expect_version`. |
| `var.get` | `swarm var get` | Absent keys read as version zero. |
| `claim.set` | `swarm claim set` | Advisory, expiring, never a lock. |
| `claim.list` | `swarm claim list` | Bounded current claims. |
| `claim.release` | `swarm claim release` | Release one advisory claim. |
| `command.submit` | `swarm launch` | Launch a child; send/wait child verbs. |
| `swarm.wait` | `swarm wait` | Wait for child commands/turns. |
| `swarm.digest` | `swarm digest` | Durable completion digest to parent. |
| `command.children` | `swarm cmd children` | Page one command's children. |
| `command.cancel` | `swarm cmd cancel` | Owner-only withdrawal, undispatched only. |
| `agent.result` | `swarm result` | Retained turns/results for one agent. |
| `swarm.handoff` | `swarm handoff` | Owner-only transfer to an agent. |
| `swarm.reparent` | `swarm reparent` | Owner-only subtree move. |
| `swarm.switch` | `swarm switch` | Owner-only launch-plus-handoff. |

Owner-only reads/writes (`settings.*`, `config.*`, `rule.*`, `timer.*`,
`goal.*`, `project.*`, `profile.*`, `daemon.*`, `command.resolve`,
`turn.resolve`, `harness.*`, `agent.discover`, `usage.*` except where the
live surface grants scope) reject agents with `not_allowed`: propose them
in the final report instead of calling them.