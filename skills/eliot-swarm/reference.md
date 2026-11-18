# Eliot Swarm reference

Use `api.methods` to discover methods available to your principal and
`api.help` for the method's fields, examples and live limits. The CLI names
below use `swarm` as shorthand for the approved release executable. Prefer
its full path if PATH resolves an older install. Check flags with the
subcommand's help. A discovered method grants no access outside your scope.

## Method-to-CLI map

| Method | CLI | Notes |
| --- | --- | --- |
| `api.identity` | `swarm identity` | Agent ID, generation and allowed scopes. |
| `api.methods`, `api.help` | MCP only | Discovery and DTO help. |
| `who` | `swarm who` | Workers, advisory claims and your marked row. |
| `agent.get` | `swarm show` | Use `--json` for the frozen profile, retained workspace and active turn. |
| `claim.set`, `claim.release` | `swarm claim set`, `swarm claim release` | Both take a positional project-relative glob. |
| `claim.list` | `swarm claim list` | Bounded scoped claims. |
| `command.submit` | `swarm launch`, `swarm send`, `swarm steer`, `swarm interrupt`, `swarm stop` | Choose the corresponding verb. |
| `command.get`, `command.children` | `swarm cmd show`, `swarm cmd children` | Read a retained command or one page of its expansion. |
| `swarm.wait` | `swarm wait` | Wait on frozen child commands or exact turn references. |
| `swarm.digest` | `swarm digest` | Register one durable digest in the parent mailbox. |
| `agent.result` | `swarm result` | Retained turns in scope. Bind with `--generation` and `--turn-id`. |
| `mail.send` | `swarm msg send` | Only queue mode is connected. |
| `mail.inbox` | `swarm inbox` | Read one unread page for your mailbox. |
| `mail.read` | `swarm msg show` | Read one exact delivery. |
| `mail.ack` | `swarm msg ack` | Acknowledge consumed deliveries with `--delivery`. |
| `var.get`, `var.set` | `swarm var get`, `swarm var set` | CAS uses `expect_version` / `--expect-version`. |
| `artifact.register` | `swarm artifact register` | Register an owned directory. |
| `cleanup.plan`, `cleanup.apply` | `swarm cleanup plan`, `swarm cleanup apply` | Agents are limited to their own tree. |
| CLI startup only | `swarm daemon` | Select the authorized private data dir explicitly. |
| `daemon.status`, `daemon.shutdown` | `swarm status`, `swarm daemon stop` | Owner-only RPCs. Prefer drain for a clean stop. |
| `harness.list`, `harness.models`, `harness.quota` | `swarm harnesses`, `swarm models`, `swarm quota` | Owner-only harness, model and account reads. |
| `project.list` | `swarm project list` | Owner-only project registration read. |
| `settings.read`, `settings.save` | `swarm settings read`, `swarm settings save` | Owner-only settings operations. |
| `rule.*`, `timer.*`, `goal.*` | `swarm rule`, `swarm timer`, `swarm goal` | Owner-only automation and Goal operations. |
| `agent.discover`, `diagnose` | `swarm discover`, `swarm why` | Owner-only discovery and diagnosis. |
| `swarm.handoff`, `swarm.reparent`, `swarm.switch` | `swarm handoff`, `swarm reparent`, `swarm switch` | Owner-only hierarchy or task transfers. |
| `command.cancel` | `swarm cmd cancel` | Owner-only withdrawal of an undispatched handoff input. |
| `command.resolve`, `turn.resolve` | `swarm cmd resolve`, `swarm turn resolve` | Owner-only outcome assertions. |

Owner-only methods can be hidden from Agent discovery. An Agent that needs
an Owner operation should propose it, including the resource and reason.
The production desktop needs Owner authority. A private Owner session is
valid only on the separately authorized private target.

## Children and results

For shared implementation or tracked delegation, use Swarm children.
Native subagents are suitable for private, read-only exploration with no
shared edits. Give each Swarm child disjoint files, an exact assignment,
and instructions to check `who`, claim before editing and acknowledge
consumed mail. Agree on the result format before launch.

Read the parent's own `agent.get` result and inspect the frozen
`profile_snapshot.child_profiles`. A child may reuse the parent snapshot
or name an allow-listed profile. Same-harness children must keep the
parent's frozen policy and credential bindings and may differ only in model
and effort. An allow-listed different harness uses its own profile policy
and bindings. A configured `worker` role selects from eligible profiles.
If a profile is refused, use an authorized compatible option or report
the refusal. A later config edit cannot widen the frozen allow-list.

For MCP launch, use `command.submit` with `verb: "launch"` and
`args` containing `parent`, `profile` and `text` for a named, prompted
child. The daemon freezes the profile snapshot and inherits the workspace
when omitted. For CLI launch in a checkout, pass the parent's exact
workspace with `--workspace`. An Agent child shares its parent's
workspace and cannot request a new worktree or broader control.

Keep the launch receipt and linked input. The launch command can settle
when the child is ready, before its first turn finishes. CLI `launch
--wait` with text waits on the linked first turn. For MCP, read the
linked input with `command.get` to capture the exact turn ID once it
exists. If you have only the launch ID, use `command.children` to find
its input command. Wait on the captured Agent, generation and turn with
`swarm.wait` or use `swarm.digest` for a durable mailbox result. Re-read the
same binding on timeout. Collect `agent.result` before integrating and
read the outcome even when the result-read command exits successfully.

## Mail and shared variables

A mail notice is a prompt to read the inbox. Read one delivery with
`mail.read`, then pass that exact `delivery_id` in `mail.ack`'s
`deliveries` list. Reading leaves it unread until acknowledged. If the
only reason for a new turn is a notice after your final report, process
the addressed mail and acknowledge it. Continue the old task only if
the mail contains a new instruction requiring it.

Agent variables default to the actual project. Agents can read global
values, and global writes require Owner authority. Read the current
value and version before computing an update. Use `expect_version: 0`
for an absent key. A definite comparison failure allows a new write
after a fresh read and recomputation. An uncertain write needs retained
readback or a same-ID retry.

## Artifacts and cleanup

Register each directory you created for the assignment under
`ELIOT_ARTIFACT_ROOT`, the root Swarm exclusively creates and durably
binds before native startup. Use child directories. A workspace or
shared configured artifact location alone grants no ownership. Include
external build targets, temp folders, screenshot folders and private
daemon data directories. Use an absolute path and a purpose.

| Kind | What to register |
| --- | --- |
| `build` | Your assignment's build or test output directory, including a private `CARGO_TARGET_DIR` outside the checkout. |
| `data_dir` | The private daemon data directory you created. |
| `temp`, `other` | Owned scratch and evidence directories. |

Swarm records managed worktrees and branches, declared scratch directories
and a frozen profile `CARGO_TARGET_DIR` from retained launch facts within
its ownership boundary. Never register the bound root itself, user/native
configuration or native stores. Hard-linked files, nested repositories,
`.git` and protected configuration can keep the whole artifact. A private
daemon lock with a live owner or a live process holding its data dir
keeps that directory.

When authorized to clean up your subtree, stop the children you own and
verify their settlement before applying. Plan with `cleanup.plan`
(`scope: "self_tree"`) or `& $swarm cleanup plan --scope self_tree`.
Review sizes and `keep` reasons. Consume that exact plan via `cleanup.apply`
with its `plan_id` and a retained mutation ID, or run
`& $swarm cleanup apply $planId --id $mutationId`. Skip apply where the
assignment authorizes review only. Owner can choose `project` or `all`.
Leave any uncertain cleanup `unknown` and retry only the identical write
to read its retained result.

Native stores stay out of filesystem cleanup. Apply archives a terminal
Codex session through the harness's `thread/archive`. Other harnesses
stay `keep(no_native_archive)`. A branch is safe only after a fresh
upstream fetch proves it merged into the upstream default with no unpushed
commits. Apply re-verifies and uses Git's safe local deletion. The Owner
must explicitly request `--delete-remote` for remote branch deletion.

## Private live proof on Windows

1. Use a fully isolated data directory within your bound artifact root.
   Set `$proofDir` to its absolute path, distinct from production, and
   create a fresh directory there. Use a group such as `qa-k123` for
   temporary repos, with plain names like `app` and `library`. Never launch
   Agents on a production copy. A separately authorized backup copy is
   suitable only for Owner read-path checks because its store can share
   production's Job custody namespace.
2. Protect the new directory's ACL. Use the current Windows account even
   when `USERNAME` is empty. Abort if account lookup or ACL setup fails.

   ```powershell
   $proofUser = whoami
   icacls $proofDir /inheritance:r /grant:r ('{0}:(OI)(CI)F' -f $proofUser)
   ```

3. From your Agent's issuing controller, register the private directory.
   Write only the private configuration needed for the proof, using the
   assignment's approved native profiles.

   ```powershell
   & $swarm artifact register $proofDir --kind data_dir --purpose 'Private live proof'
   ```

4. Start the private daemon. Check readiness and target identity, then
   run the real task with the selected native profile. Pass
   `--data-dir $proofDir` on every private command and leave the production
   Agent environment intact.

   ```powershell
   & $swarm daemon --data-dir $proofDir
   & $swarm identity --data-dir $proofDir
   & $swarm launch --data-dir $proofDir --profile $proofProfile --workspace $proofWorkspace --wait --timeout-seconds 30 $task
   ```

5. Close test desktop windows immediately after taking screenshots.
   If a screenshot is black, check whether the console display is off and
   recapture usable evidence. Stop owned test Agents in the private
   directory, request drain, and check status and owned process exit
   before reporting teardown. If drain reports blockers, read the
   blocker state and account for the exact owned processes. Never kill
   processes by name or operate on the production daemon.

   ```powershell
   & $swarm stop $child --data-dir $proofDir --generation $childGeneration
   & $swarm daemon stop --data-dir $proofDir --drain
   & $swarm status --data-dir $proofDir
   ```

   From a private Owner MCP connection, `daemon.shutdown` with
   `mode: "drain"` requests the same stop and `daemon.status` gives readback
   while the daemon is reachable. Confirm exit separately. Keep the proof
   and teardown in this turn. Use foreground commands with timeouts or
   poll a running command while the turn is active. A native background
   completion notification does not start a new Swarm turn.

## Unknown outcomes

Retain a mutation ULID before the first write when possible. CLI writes
accept `--id` and print a generated ID before sending. MCP can mint one
and return it with the reply. If a reply is lost, read back the same
command, turn, delivery or other retained state. Retry only the same
mutation ID, principal, method and semantic payload. If the ID is missing
or the effect remains unproven, report `unknown`. Never issue a new
mutation to replay an uncertain effect or treat acceptance as completion.
