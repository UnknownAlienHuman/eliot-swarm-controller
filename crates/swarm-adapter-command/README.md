# Standalone Command adapter version 3

This crate is the standalone Rust module for one-shot native Command Code runs. It does not link the monolithic controller crate. Version 3 adds `agent.result` with normalized result context/page schemas for bounded status and captured-output pages. Captured output remains raw CLI bytes; the adapter does not assert a native assistant-response identity, completed model execution, or Task acceptance.

## Two configuration inputs

The descriptor's `launch.argv` contains two different config arguments:

1. `--module-host-config` followed by the typed `module_host_config_path` schema-v1 marker. The supervisor replaces that marker at launch with a private per-binding file containing the host data directory and IPC settings. Leave the marker unchanged in the descriptor.
2. `--config` followed by an absolute path to an operator-maintained native Command JSON file. This file is not copied by the module installer and is not inside the descriptor's install receipt.

Start from `config/swarm-adapter-command-v3.example.json`. The version-3 native JSON contains exactly the fields needed to invoke the configured local executable and preserved Command mod; the adapter rejects missing or unknown fields:

```json
{
  "module_artifact_id": "eliot-command.rust-headless.1",
  "command": "C:\\ELIOT\\bin\\command.exe",
  "command_args": [],
  "mod_path": "C:\\ELIOT\\mods\\command\\.3\\command.js",
  "run_timeout_ms": 1800000
}
```

Replace the sample paths with the actual absolute paths before launch. `command` and `mod_path` must be absolute; Windows `.cmd` and `.bat` command wrappers are rejected. `command_args` may contain at most 32 absolute fixed arguments, each no longer than 4096 UTF-8 bytes; they cannot supply the adapter-owned print, model, working-directory, or mod flags. The configured mod must match the version-3 artifact's SHA-256 after CRLF line endings are normalized. `run_timeout_ms` must be between 100 and 86,400,000. The model and workspace come from the admitted route/operation. Keep native credentials in the native tool's existing local credential store; do not put them in this file, the descriptor, or the module launch plan.

## Install version 3

Copy `module-descriptor.template.json` outside the checkout, set `enabled` to `true` only when ready to make this version selectable, replace `launch.credential_ref` with the opaque protected reference provisioned for this module, and replace the last `launch.argv` literal with the absolute path to the native JSON file. Do not replace the typed host-config marker. The generic installer fills in the executable path and executable SHA-256 and preserves the marker. It rejects unresolved `<INSTALLER_...>` tokens.

Run `tools/modules/Install-ModuleArtifact.ps1` with the already-built `swarm-adapter-command.exe`, the edited descriptor, and an existing dedicated install root. The install receipt binds the binary and exact resulting descriptor. It does not copy, hash, register, or launch the operator-maintained native JSON or native Command files. A changed executable or descriptor requires a new artifact version; changing the separately maintained native JSON is an operator configuration change.

`module-descriptor-v2.template.json` and `config/swarm-adapter-command.example.json` retain the old version-2 descriptor/config contract for installations that still use that binary. Version 3 has the same artifact ID at version `3`, uses `config/swarm-adapter-command-v3.example.json`, and opts into normalized result pages for `agent.result`; select version 3 explicitly for future bindings. Existing version-2 bindings are not rewritten.

## Result boundary

Version 3 returns bounded `command_status` facts or pages from the exact captured `stdout.ndjson` and `stderr.txt` files. Store seals the selected dispatch, module receipt, stream digest, byte length, and completeness at result admission; the adapter reads only that immutable journal capture, and Store rechecks the page against the sealed digest. Incomplete, truncated, or unreadable captures cannot become complete candidates. The returned bytes are raw native CLI output and carry no inferred assistant identity or Task-completion claim; normalized pages keep `execution_complete: false`, `task_completion: "unknown"`, and `native_replay: false`. The existing Manager submission path may use a complete assembled body as a candidate, while Task acceptance remains a separate Manager decision. Unknown native work is never replayed to manufacture a result.
