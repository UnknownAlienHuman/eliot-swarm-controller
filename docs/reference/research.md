# Research

Findings that shape the design, with the rule each one supports. Entries marked ✓
were re-read on 2026-10-25; the others are carried over from v1's source notes
(`git show v1-final:docs/agent-communication-fleet-scale-sources.md`,
`...-field-evidence.md`, `...-peer-autonomy-sources.md`) and were not re-checked.

## Multi-agent scaling and coordination

| Source | Finding | Design rule |
| --- | --- | --- |
| ✓ [When Agents Coordinate: Measuring Coordination in Multi-Agent AI Coding](https://arxiv.org/abs/2608.16801) (arXiv, 2026-08) | Direct messages grow nearly quadratically with team size; shared files instead of repeated 1:1 messages cut output tokens ~42% at 8 agents on message-heavy work; naming a coordinator gave no reliable gain. | Shared vars and `who` first, messages second ([coordination](../spec/coordination.md#communication-guidance-for-agent-prompts)); no mandatory manager relay. |
| ✓ [Towards a Science of Scaling Agent Systems](https://arxiv.org/abs/2512.08296) (arXiv, 2025-12) | Coordination helps less when one agent is already strong; adds overhead on tool-heavy tasks; architecture–task fit decides (+80.8% to −70.0%); no central verification spreads errors. | Parallelism is a per-task choice, not a default; verification (checks, goal `check`) belongs to the workflow. |
| ✓ [Building a C compiler with a team of parallel Claudes](https://www.anthropic.com/engineering/building-c-compiler) (Anthropic, 2026-02) | Parallel agents work on many independent, verifiable items; on one monolithic task they overwrite each other. Coordination was lock files in the repo plus Git sync, near-perfect tests, CI, concise output with logs in files. | Claims + Git-based `who`; tests as goal checks; quiet agents. |
| [How we built our multi-agent research system](https://www.anthropic.com/engineering/multi-agent-research-system) (Anthropic) | Large gains on breadth-first research at ~15× the tokens; long-running agents need checkpoints and resume. | Budgets on goals; `resume` instead of restart. |
| [Agensh: 1,024 agents](https://arxiv.org/abs/2609.26781) (arXiv, 2026-09) | 1,024 agents only modestly beat 128 (55.06% vs 50.94%). | "A million agents" is a capacity property, not a target. |
| [Diversity Collapse in Multi-Agent LLM Systems](https://aclanthology.org/2026.findings-acl.13/) | Dense communication causes premature convergence. | Default to sparse, addressed messages. |
| [The Cost of Consensus](https://arxiv.org/abs/2605.00914), [The Consistency Illusion](https://arxiv.org/abs/2606.08457), [Free-MAD](https://arxiv.org/abs/2509.11035) | Debate/consensus spends 2–3× tokens for equal or lower accuracy; agreement is not correctness. | No voting/council features; checks decide. |
| [AgentPrune](https://arxiv.org/abs/2410.02506), [Adaptive Graph Pruning](https://arxiv.org/abs/2506.02951), [CONCAT](https://arxiv.org/abs/2605.29612) | Pruning low-value communication saves large token shares at equal quality. | Message only specific recipients; broadcast is explicit (`@project`). |
| [AgentLeak](https://arxiv.org/abs/2602.11510) | Inter-agent channels leak data that output-only checks miss. | Secrets never in messages, vars or events. |

## Field failures in other systems

| Source | Failure | Design rule |
| --- | --- | --- |
| [Multica #8753](https://github.com/multica-ai/multica/issues/8753) | Status/failure comments triggered full agent turns: ping-pong, ~18M tokens. | Self-trigger guard and loop depth limit ([automation](../spec/automation.md#guardrails)). |
| [MCP Agent Mail #264](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/issues/264), [#334](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/issues/334) | Liveness chatter was ~92% of mail; unknown recipients created identities. | Status is on the bus, not in mail; unknown recipient = error. |
| AutoGen [#108](https://github.com/microsoft/autogen/issues/108), [#1006](https://github.com/microsoft/autogen/issues/1006), [#7321](https://github.com/microsoft/autogen/issues/7321) | Politeness loops, full-history amplification, retry storms. | No acks by message; bounded delivery; no automatic retries of uncertain effects. |
| Claude Code [#68110](https://github.com/anthropics/claude-code/issues/68110), [#66686](https://github.com/anthropics/claude-code/issues/66686), [#99111](https://github.com/anthropics/claude-code/issues/99111) | Recursive child fan-out; poor fleet visibility; stored delivery ≠ shown to a busy model. | `max_depth`; `eliot top`; delivery `mode` reports how text reached the agent. |
| [openai/codex#32254](https://github.com/openai/codex/issues/32254) | No idempotency key for app-server input (open proposal). | `unknown` + readback, never resend. |
