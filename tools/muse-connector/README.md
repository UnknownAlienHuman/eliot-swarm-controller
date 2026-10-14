# Muse connector for the Eliot Swarm Controller

A custom connector that lets Muse (Meta's personal agent) work with this
controller directly: check the repository state on GitHub and call a running
local host through its authenticated IPC — without opening the Store
database, a network listener, or a second writer path.

`swarm_connector.py` is a single Python file, standard library only. It is a
faithful port of the `swarm-client` semantics that matter to a client:

- **Endpoint discovery** identical to `crates/swarm-client/src/endpoint.rs`:
  Unix `<canonical data-dir>/control.sock` (100-byte socket-path bound);
  Windows `\\.\pipe\eliot-swarm-<SHA-256 prefix of the lowercased canonical
  path>`. (The pipe *name* is derived on every platform; the pipe transport
  itself is Windows-only and left to the `swarm` CLI — this connector speaks
  the Unix socket transport and says so instead of failing obscurely.)
- **Handshake**: the first exchange is `client.hello` with the credential
  object `{"client_id", "token"}` — the exact shape of
  `swarm-contracts::Credential` (unknown fields are rejected on both sides).
- **Framing**: one JSON-RPC 2.0 object per line, 1 MiB frame bound,
  15 s write timeout, 60 s reply timeout — the `IpcConfig` defaults.
- **No blind retries**: a failed write, a missing/malformed reply, or a reply
  whose `id` does not match is `OUTCOME_UNKNOWN`. The request may have been
  applied; inspect the original operation before retrying. A failed
  connection is poisoned and never reused. Each CLI invocation opens exactly
  one connection.
- **Errors**: application failures surface the stable code from
  `error.data.code` (exit 5). Other exit codes: 2 usage, 3 host unavailable,
  4 handshake failed, 6 outcome unknown, 7 frame too large.

## Usage

```sh
# Offline protocol selftest (fake host, real wire format) — 9 checks
python3 tools/muse-connector/swarm_connector.py selftest

# Where would the host for this data directory listen?
python3 tools/muse-connector/swarm_connector.py endpoint --data-dir /path/to/.swarm-controller

# Read-only diagnostics against a running host
python3 tools/muse-connector/swarm_connector.py status --data-dir DIR --credential cred.json
python3 tools/muse-connector/swarm_connector.py doctor --data-dir DIR --credential cred.json

# Any application method from the METHOD_REGISTRY
# (crates/swarm-contracts/src/method_policy.rs)
python3 tools/muse-connector/swarm_connector.py call --data-dir DIR --credential cred.json \
    task.get --params '{"task_id":"..."}'

# Repository state on GitHub (read-only; GITHUB_TOKEN env optional)
python3 tools/muse-connector/swarm_connector.py repo-check
```

The credential file is read locally and used only in the local handshake;
it is never printed, logged, or sent off the machine. The connector holds
no policy of its own: role, GM-epoch, and object checks remain the host's,
and a `FORBIDDEN` answer is the host's decision, not a connector failure.

The same source is installed as Muse's workspace skill
`eliot-swarm-controller` (`bin/swarm_connector.py`); keep the copies in sync.
