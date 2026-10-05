# Standalone Command adapter version 3

This crate is the standalone Rust module for one-shot native Command Code runs. It does not link the monolithic controller crate. The version-3 descriptor adds the bounded `agent.result` status-page method; it does not claim a native assistant-response identity, completed model execution, or Task acceptance.

## Two configuration inputs

The descriptor's `launch.argv` contains two different config arguments:

1. `--module-host-config` followed by the typed `module_host_config_path` schema-v1 marker. The supervisor replaces that marker at launch with a private per-binding file containing the host data directory and IPC settings. Leave the marker unchanged in the descriptor.
2. `--config` followed by an absolute path to an operator-maintained native Command JSON file. This file is not copied by the module installer and is not inside the descriptor's install receipt.

Start from `config/swarm-adapter-command-v3.example.json`. The version-3 native JSON contains only the fields needed to invoke the configured local executable and preserved Command mod:

```json
{
  "module_artifact_id": "eliot-command.rust-headless.1",
  "command": "C:\\ELIOT\\bin\\command.exe",
  "command_args": [],
  "mod_path": "C:\\ELIOT\\mods\\command\\.3\\command.js",
  "run_timeout_ms": 1800000
}
```

Replace the sample angle-bracket paths with the actual absolute paths before launch. `command` and `mod_path` must be absolute. `command_args` is a bounded list of absolute fixed arguments; it cannot supply the adapter-owned print, model, working-directory, or mod flags. The model and workspace come from the admitted route/operation. Keep native credentials in the native tool's existing local credential store; do not put them in this file, the descriptor, or the module launch plan.

## Install version 3

Copy `module-descriptor.template.json` outside the checkout, set `enabled` to `true` only when ready to make this version selectable, replace `launch.credential_ref` with the opaque protected reference provisioned for this module, and replace the last `launch.argv` literal with the absolute path to the native JSON file. Do not replace the typed host-config marker. The generic installer fills in the executable path and executable SHA-256 and preserves the marker. It rejects unresolved `<INSTALLER_...>` tokens.

Run `tools/modules/Install-ModuleArtifact.ps1` with the already-built `swarm-adapter-command.exe`, the edited descriptor, and an existing dedicated install root. The install receipt binds the binary and exact resulting descriptor. It does not copy, hash, register, or launch the operator-maintained native JSON or native Command files. A changed executable or descriptor requires a new artifact version; changing the separately maintained native JSON is an operator configuration change.

`module-descriptor-v2.template.json` and `config/swarm-adapter-command.example.json` retain the old version-2 descriptor/config contract for installations that still use that binary. Version 3 has the same artifact ID at version `3`, uses `config/swarm-adapter-command-v3.example.json`, and adds only `agent.result`; select version 3 explicitly for future bindings. Existing version-2 bindings are not rewritten.

## Result boundary

Version 3 can return a bounded `command_status` page based on the exact admitted Operation and retained Store outcome. The page keeps `native_response_identity` as `unavailable`, `execution_complete` as `false`, `task_completion` as `unknown`, and `native_replay` as `false`. It contains no inferred assistant text or model-response ID. Unknown, incomplete, or otherwise unproven native work remains unresolved and is never rerun to manufacture a result. Manager/Task acceptance remains a separate authority.
