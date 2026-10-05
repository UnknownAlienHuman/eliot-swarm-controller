set windows-shell := ["pwsh.exe", "-NoProfile", "-Command"]

default:
    @just --list

metadata:
    cargo metadata --locked --no-deps --format-version 1

# Scoped developer gate: format and strict Clippy only for changed packages
# plus their actual local reverse dependencies. BASE and HEAD are full commit SHAs.
verify BASE HEAD:
    pwsh -NoProfile -File tools/ci/package-scope.ps1 -Stage Verify -BaseSha "{{BASE}}" -HeadSha "{{HEAD}}"

fmt:
    cargo fmt -p eliot-swarm-controller -- --check

fmt-package package:
    cargo fmt --package "{{package}}" -- --check

check:
    cargo check --locked --lib --bins

clippy:
    cargo clippy --locked --lib --bins --no-deps -- -D warnings

clippy-package package:
    cargo clippy --locked --package "{{package}}" --lib --bins --no-deps -- -D warnings

test:
    cargo test --locked -p eliot-swarm-controller --lib --bins

test-target package target:
    cargo test --locked --package "{{package}}" --test "{{target}}"

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

build:
    cargo build --locked --release --bin swarm

full-rust:
    pwsh -NoProfile -File tools/ci/package-scope.ps1 -Stage FullRust

# Explicit full local qualification; scoped PR checks do not call this recipe.
verify-full: full-rust bridge-fixtures codex-fixtures opencode-fixtures build
