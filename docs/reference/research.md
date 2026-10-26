# Research

Findings that materially constrain the design, with the rule each supports. Entries
marked ✓ were reread on 2026-10-25. Unmarked entries come from v1 source notes and
must be rechecked before their exact numbers are used in implementation/release text.

## Multi-agent scaling and coordination

| Source | Finding | Design rule |
| --- | --- | --- |
| ✓ [When Agents Coordinate: Measuring Coordination in Multi-Agent AI Coding](https://arxiv.org/abs/2608.16801) (2026-08) | Direct messages grow nearly quadratically with team size; shared files reduced repeated message tokens materially at eight agents; naming a coordinator gave no reliable gain in that setup. | Shared vars and `who` first, addressed mail second; no mandatory manager relay ([prompt guidance](../spec/coordination.md#prompt-guidance)). |
| ✓ [Towards a Science of Scaling Agent Systems](https://arxiv.org/abs/2512.08296) (2025-12) | Coordination gains depend on task/architecture; strong single agents and tool-heavy work can lose from coordination overhead. | Parallelism is a task choice, not default. Verification is an explicit workflow/check. |
| ✓ [Building a C compiler with a team of parallel Claudes](https://www.anthropic.com/engineering/building-c-compiler) (2026-02) | Parallel workers fit independent, verifiable work; monolithic work creates overwrite/conflict pressure. The project relied on Git synchronization and strong tests. | One manager/worktree, advisory claims + Git `who`, bounded assignments and later qualification/checks. |
| [How we built our multi-agent research system](https://www.anthropic.com/engineering/multi-agent-research-system) | Breadth-first research can benefit while spending substantially more tokens; long work needs checkpoints/resume. | Goal budgets and explicit resume/rotation. |
| [Agensh: 1,024 agents](https://arxiv.org/abs/2609.26781) | Scaling from 128 to 1,024 agents produced modest extra gain in its benchmark. | A million agents is a capacity property, not a target team size. |
| [Diversity Collapse in Multi-Agent LLM Systems](https://aclanthology.org/2026.findings-acl.13/) | Dense communication can cause premature convergence. | Sparse addressed mail by default. |
| [The Cost of Consensus](https://arxiv.org/abs/2605.00914), [The Consistency Illusion](https://arxiv.org/abs/2606.08457), [Free-MAD](https://arxiv.org/abs/2509.11035) | Debate/consensus may spend more tokens without improving correctness; agreement is not verification. | No council/voting product feature; checks/evidence decide. |
| [AgentPrune](https://arxiv.org/abs/2410.02506), [Adaptive Graph Pruning](https://arxiv.org/abs/2506.02951), [CONCAT](https://arxiv.org/abs/2605.29612) | Pruning low-value communication can preserve quality while reducing traffic. | Specific recipients; project broadcast is explicit and guarded. |
| [AgentLeak](https://arxiv.org/abs/2602.11510) | Inter-agent channels create leakage not captured by output-only checks. | Secrets never enter mail, vars, events or fixtures. |

## Field failures in other systems

| Source | Failure | Design rule |
| --- | --- | --- |
| [Multica #8753](https://github.com/multica-ai/multica/issues/8753) | Status/failure comments recursively triggered model turns and large token waste. | Self-trigger/rate/depth guard ([automation guardrails](../spec/automation.md#guardrails)). |
| MCP Agent Mail [#264](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/issues/264), [#334](https://github.com/Dicklesworthstone/mcp_agent_mail_rust/issues/334) | Liveness chatter dominated mail; unknown recipients created identities. | Status stays on bus; unknown recipient is error; wake is coalesced. |
| AutoGen [#108](https://github.com/microsoft/autogen/issues/108), [#1006](https://github.com/microsoft/autogen/issues/1006), [#7321](https://github.com/microsoft/autogen/issues/7321) | Politeness loops, full-history amplification and retries. | No acknowledgements by default; bounded mail; no retry of uncertain effects. |
| Claude Code [#68110](https://github.com/anthropics/claude-code/issues/68110), [#66686](https://github.com/anthropics/claude-code/issues/66686), [#99111](https://github.com/anthropics/claude-code/issues/99111) | Recursive fan-out, weak fleet visibility and stored delivery not shown to a busy model. | Fan-out guards, `eliot top`, separate admitted/presented/read mail states. |
| [openai/codex#32254](https://github.com/openai/codex/issues/32254) | App-server input had no qualified native idempotency key at review time. | Eliot admission id + unknown/readback; never resend native input merely because reply was lost. |
