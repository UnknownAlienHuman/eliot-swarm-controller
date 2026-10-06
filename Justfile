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
    cargo fmt -p swarm-cli -- --check

fmt-package package:
    cargo fmt --package "{{package}}" -- --check

check TARGET=shared_target: (_shared-target TARGET)
    cargo check --locked --package eliot-swarm-controller --lib --bins --target-dir "{{TARGET}}"
    cargo check --locked --package swarm-cli --lib --bins --target-dir "{{TARGET}}"
    cargo check --locked --package swarm-supervisor --lib --bins --target-dir "{{TARGET}}"

clippy TARGET=shared_target: (_shared-target TARGET)
    cargo clippy --locked --package eliot-swarm-controller --lib --bins --no-deps --target-dir "{{TARGET}}" -- -D warnings
    cargo clippy --locked --package swarm-cli --lib --bins --no-deps --target-dir "{{TARGET}}" -- -D warnings
    cargo clippy --locked --package swarm-supervisor --lib --bins --no-deps --target-dir "{{TARGET}}" -- -D warnings

clippy-package package TARGET=shared_target: (_shared-target TARGET)
    cargo clippy --locked --package "{{package}}" --lib --bins --no-deps --target-dir "{{TARGET}}" -- -D warnings

test TARGET=shared_target: (_shared-target TARGET)
    cargo build --locked --package swarm-kernel-host --bin swarm-kernel-host --target-dir "{{TARGET}}"
    cargo build --locked --package swarm-supervisor --bin swarm-supervisor --target-dir "{{TARGET}}"
    cargo test --locked -p swarm-kernel-host --lib --bins --target-dir "{{TARGET}}"
    cargo test --locked -p swarm-supervisor --lib --bins --target-dir "{{TARGET}}"
    cargo test --locked -p eliot-swarm-controller --lib --bins --target-dir "{{TARGET}}"
    cargo test --locked -p swarm-cli --lib --bins --target-dir "{{TARGET}}"

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
    cargo build --locked --release --package swarm-cli --bin swarm --target-dir "{{TARGET}}"
    cargo build --locked --release --package eliot-swarm-controller --bin swarm-host --target-dir "{{TARGET}}"
    cargo build --locked --release --package swarm-kernel-host --bin swarm-kernel-host --target-dir "{{TARGET}}"
    cargo build --locked --release --package swarm-supervisor --bin swarm-supervisor --target-dir "{{TARGET}}"

# Create one provenance package at a time. The package builders require a
# caller-owned external shared target and a fresh, separate output directory.
package-module package profile target output: (_shared-target target)
    pwsh -NoProfile -File tools/ci/build-module-package.ps1 -Package "{{package}}" -Profile "{{profile}}" -TargetDir "{{target}}" -OutputDir "{{output}}"

package-frontend package target output: (_shared-target target)
    pwsh -NoProfile -File tools/ci/Build-SwarmFrontendProvenance.ps1 -Package "{{package}}" -TargetDir "{{target}}" -OutputDir "{{output}}"

package-host target output: (_shared-target target)
    pwsh -NoProfile -File tools/ci/Build-SwarmHostProvenance.ps1 -TargetDir "{{target}}" -OutputDir "{{output}}"

package-kernel-host target output: (_shared-target target)
    pwsh -NoProfile -File tools/ci/Build-SwarmKernelHostProvenance.ps1 -TargetDir "{{target}}" -OutputDir "{{output}}"

package-supervisor target output: (_shared-target target)
    pwsh -NoProfile -File tools/ci/Build-SwarmSupervisorProvenance.ps1 -TargetDir "{{target}}" -OutputDir "{{output}}"

full-rust TARGET=shared_target:
    pwsh -NoProfile -File tools/ci/package-scope.ps1 -Stage FullRust -TargetDir "{{TARGET}}"

# Explicit full local qualification; scoped PR checks do not call this recipe.
verify-full TARGET=shared_target: (full-rust TARGET) bridge-fixtures codex-fixtures opencode-fixtures (build TARGET)
