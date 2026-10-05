set windows-shell := ["pwsh.exe", "-NoProfile", "-Command"]

# All compiling recipes share an explicit external Cargo cache. An empty value
# fails before Cargo can allocate a checkout-local default target.
shared_target := env_var_or_default("CARGO_TARGET_DIR", "")

_shared-target TARGET:
    pwsh -NoProfile -File tools/ci/package-scope.ps1 -Stage ValidateTarget -TargetDir "{{TARGET}}"

default:
    @just --list

metadata:
    cargo metadata --locked --no-deps --format-version 1

# Scoped developer gate: format and strict Clippy only for changed packages
# plus their actual local reverse dependencies. BASE and HEAD are full commit SHAs.
verify BASE HEAD TARGET=shared_target:
    pwsh -NoProfile -File tools/ci/package-scope.ps1 -Stage Verify -BaseSha "{{BASE}}" -HeadSha "{{HEAD}}" -TargetDir "{{TARGET}}"

fmt:
    cargo fmt -p eliot-swarm-controller -- --check

fmt-package package:
    cargo fmt --package "{{package}}" -- --check

check TARGET=shared_target: (_shared-target TARGET)
    cargo check --locked --lib --bins --target-dir "{{TARGET}}"

clippy TARGET=shared_target: (_shared-target TARGET)
    cargo clippy --locked --lib --bins --no-deps --target-dir "{{TARGET}}" -- -D warnings

clippy-package package TARGET=shared_target: (_shared-target TARGET)
    cargo clippy --locked --package "{{package}}" --lib --bins --no-deps --target-dir "{{TARGET}}" -- -D warnings

test TARGET=shared_target: (_shared-target TARGET)
    cargo test --locked -p eliot-swarm-controller --lib --bins --target-dir "{{TARGET}}"

test-target package target TARGET=shared_target: (_shared-target TARGET)
    cargo test --locked --package "{{package}}" --test "{{target}}" --target-dir "{{TARGET}}"

# These fixtures exercise native contracts; they never call a real model.
bridge-fixtures:
    node modules/muse/selftest.mjs
    node modules/claude/selftest.mjs
    node modules/antigravity/selftest.mjs
    node modules/command/test-glue.mjs
    node modules/command/test-bridge.mjs
    node modules/opencodex/selftest.mjs

# Requires the existing pinned module-local Python dependencies.
codex-fixtures:
    python modules/codex/verify_vendor.py
    python modules/codex/test_bridge.py

# Requires module-local npm ci and the absolute Bun 1.4.0 path in ELIOT_OPENCODE_BUN_EXE.
opencode-fixtures:
    node modules/opencode/selftest.mjs

build TARGET=shared_target: (_shared-target TARGET)
    cargo build --locked --release --bin swarm --target-dir "{{TARGET}}"

full-rust TARGET=shared_target:
    pwsh -NoProfile -File tools/ci/package-scope.ps1 -Stage FullRust -TargetDir "{{TARGET}}"

# Explicit full local qualification; scoped PR checks do not call this recipe.
verify-full TARGET=shared_target: (full-rust TARGET) bridge-fixtures codex-fixtures opencode-fixtures (build TARGET)
