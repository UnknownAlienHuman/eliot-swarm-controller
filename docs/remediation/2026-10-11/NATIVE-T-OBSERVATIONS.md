# Retained native T observations

Status: partial installed evidence; neither adapter has full model qualification.

These runs used frozen commit `3449eefa32cb2043d95b7ebee2fd47cfa84d7f60`,
tree `76db5e7fefaf561646c076ba51bd6795a3fbd72a`. They do not qualify the
later watcher delivery or current main as a whole. No run is replayed.

| Run | Actual result | Remaining evidence |
| --- | --- | --- |
| OpenCode `48192e3c-ab92-4486-853d-2857cd87eeff` | Exact agent.open `94602145-cf63-4653-a5e2-154adda374b8` rejected with `NATIVE_MODEL_UNAVAILABLE / model_absent`; zero dispatches | Actual selected server catalog and requested native ID/variant mapping; fresh execution after resolving it |
| Antigravity `da0bced4-e8ab-477e-ada7-1a2beac98bb4` | One dispatch `e38cd2c7-3af4-4b20-8217-1bb032d8bf5b`, settled/applied; native init verifies `gemini-3.8-flash-high` | Causally bound result body and independent Task acceptance |

The Antigravity Task `3daab491-2f12-4d0f-8bbe-70ca4353f81e` remains open with
no accepted Attempt. Retained runtime.state reports verified model readback,
nine native events, one result ordinal/turn, four steps and zero gaps. These
are status observations. The adapter's public result surface does not retain
native request/assistant-message identity or response text; those observations
cannot prove causal result-body consumption.

The Antigravity receipt remains at SHA-256
`f0f2c4b754ce7ccb24f32d2cf38ccfdbbcfc313869db5c9936e67e6cbf1b2d6b`.
It recorded EOF sent with host PID50636 still pending. A read-only observation
at `2026-10-11T08:49:21Z` found that PID and matching installed host absent;
exec session8300 also no longer exists. No numeric exit status was recovered.
The historical pending receipt is not rewritten or upgraded to successful
cleanup, and no force termination or restart was performed.

The [observation repair](QUALIFICATION-OBSERVATION-REPAIR.md) fixes current
harness reporting separately from these frozen historical results.

The installed core 2.0.7 source names the unauthenticated public catalog
`https://models.opencode.ai/api.json`. Its live readback at
`2026-10-11T08:56:33Z` lists `providerID=kilo`, native
`id=stepfun/step-5-preview-free` and `high` reasoning effort. The shipped
snapshot lacks Step 5 free. The failed run requested the flattened id
`stepfun-step-5-preview-free`, and the current qualification parameter/parser
cannot express the native slash-containing id. Correcting that coordinate
preserves the requested Step 5 free model; it is not a substitute model.
The failed run's specific server catalog was not recovered, and public catalog
visibility alone does not establish that server's readiness or authorization.
The existing retained Store diagnosis proves the rejected-open category
separately from the older timeout receipt.
