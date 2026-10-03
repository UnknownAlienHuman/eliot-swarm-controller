# Native Forge publication, 2026-10-03

One real controller-managed publication advanced GitHub `refs/heads/main` from
`35e499ae73b622d873c44873f6993ee3fcbea87b` to
`504199d14135c030ad3951a3c5023a098a3d03f0`. The push was non-force and its exact
result was independently read back from the unchanged configured push endpoint.
This qualifies the tested publication path; it does not establish whole-project
completion, mixed-provider inference, or performance targets.

The isolated host used R6 source `34bd4a42ebd7f15cc5ad6099f406d1c9cf3b8c6b`
and binary SHA-256
`96c46cc6fbab070dc81d4ec5d0b5132e6f2f0b77eb7b2ce720632608a6ffde28`.
Authenticated Task creation, claim, exact Git source capture, submission,
CheckRunner execution, independent acceptance and Forge publication used the
native controller API. No acceptance record was seeded through SQLite. The
source manifest matched all 380 regular files in the candidate Git tree; shared
uncommitted work was excluded. No model request was involved.

The required `codex-fixture/1` check ran the existing Python 3.13 interpreter
against the captured source, passed 13 tests with exit code zero, and retained
complete coverage, stdout, stderr and source-verification evidence. Its CheckRun
is `1eba1029-3c07-4d6d-9e72-98e92ce3d8c5`; independent operator acceptance is
`4922cd56-3dce-4036-afcd-369486678b37`. Exact source reviews used GPT Luna at
maximum reasoning effort.

Publication Operation `8daf281e-f069-4097-9182-61e7bf67dd3c` settled as applied
with `publication=confirmed_by_remote_readback` and `force=false`. The reviewed
request digest was
`c3e2ee643b77ca9c159d66b0bb48bd64f08d55ffc058a209755596213d9087e9`.
An exclusive one-shot marker preceded the only `forge.publish_ref` call; there
was no mutation retry. A separate Git fetch subsequently confirmed the same
remote commit.

The preceding candidate `8438565a42947d80ff0ec3fc8f1686097e9ce992` also passed
the 13 tests but correctly remained incomplete: its fixture spawned a child
Python without `-B`, generating 20 bytecode files in the captured source tree.
The new candidate added `-B` to that child invocation. The original incomplete
CheckRun and changed checkout remain retained; neither was relabeled or repaired
in place. Detailed receipts and private state remain in the protected local
qualification directory and are not published with this document.

The isolated Forge host was stopped after confirming zero queued work and no
native bindings. Its exact process handle, executable and creation time were
checked before the stop; the retained Task, CheckRun, acceptance and publication
receipts were preserved. Original desktop Codex processes remained alive.
