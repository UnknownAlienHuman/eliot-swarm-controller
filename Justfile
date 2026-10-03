set windows-shell := ["pwsh.exe", "-NoProfile", "-Command"]

default:
    @just --list

metadata:
    cargo metadata --locked --no-deps --format-version 1

fmt:
    cargo fmt -p eliot-swarm-controller -- --check

check:
    cargo check --locked --lib --bins

clippy:
    cargo clippy --locked --lib --bins --no-deps -- -D warnings

test:
    cargo test --locked -p eliot-swarm-controller --lib --bins

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

build:
    cargo build --locked --release --bin swarm

# Install the documented locked module SDKs before invoking this gate.
verify: fmt clippy test bridge-fixtures codex-fixtures build
