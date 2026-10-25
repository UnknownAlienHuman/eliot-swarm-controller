#!/usr/bin/env python3
"""Inventory the first incomplete source-audit cluster.

This records syntax-level facts for root entrypoints, swarm-cli, swarm-kernel,
swarm-bus and swarm-telemetry. It deliberately does not infer runtime correctness from
names or search hits; semantic conclusions live in the reviewed audit checkpoint.
"""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
import tomllib
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT_JSON = ROOT / "docs/agent-operations/source-surface-inventory.json"
OUT_MD = ROOT / "docs/agent-operations/source-surface-inventory.md"

CLUSTERS = {
    "root-entrypoints": [ROOT / "src"],
    "swarm-cli": [ROOT / "crates/swarm-cli/src"],
    "swarm-kernel": [ROOT / "crates/swarm-kernel/src"],
    "swarm-bus": [ROOT / "crates/swarm-bus/src"],
    "swarm-telemetry": [ROOT / "crates/swarm-telemetry/src"],
}
PACKAGE_BY_CLUSTER = {
    "root-entrypoints": "eliot-swarm-controller",
    "swarm-cli": "swarm-cli",
    "swarm-kernel": "swarm-kernel",
    "swarm-bus": "swarm-bus",
    "swarm-telemetry": "swarm-telemetry",
}

PUB_ITEM_RE = re.compile(
    r"^\s*pub(?:\([^)]*\))?\s+(?:(?:async|const|unsafe)\s+)*(fn|struct|enum|trait|type|mod|use)\s+([A-Za-z_][A-Za-z0-9_]*)",
    re.MULTILINE,
)
FN_RE = re.compile(
    r"^\s*(?:pub(?:\([^)]*\))?\s+)?(?:(?:async|const|unsafe)\s+)*fn\s+([A-Za-z_][A-Za-z0-9_]*)",
    re.MULTILINE,
)
MODULE_RE = re.compile(r"^\s*(?:pub\s+)?mod\s+([A-Za-z_][A-Za-z0-9_]*)\s*;", re.MULTILINE)
COMMAND_ENUM_RE = re.compile(r"enum\s+Command\s*\{(?P<body>.*?)^\}", re.MULTILINE | re.DOTALL)
VARIANT_RE = re.compile(r"^\s*([A-Z][A-Za-z0-9_]*)\s*(?:\{|\(|,)", re.MULTILINE)
STRING_METHOD_RE = re.compile(r'"([a-z][a-z0-9_.-]+)"')

MARKERS = [
    "legacy",
    "compatibility",
    "historical",
    "deprecated",
    "standalone",
    "delegate",
    "wrapper",
    "TODO",
    "FIXME",
]


def run_git(*args: str) -> str:
    return subprocess.check_output(["git", *args], cwd=ROOT, text=True).strip()


def rel(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


def rust_files(paths: list[Path]) -> list[Path]:
    files: list[Path] = []
    for path in paths:
        files.extend(path.rglob("*.rs"))
    return sorted(files)


def source_digest(files: list[Path]) -> str:
    digest = hashlib.sha256()
    for path in files:
        encoded = rel(path).encode()
        data = path.read_bytes()
        digest.update(len(encoded).to_bytes(4, "big"))
        digest.update(encoded)
        digest.update(len(data).to_bytes(8, "big"))
        digest.update(data)
    return digest.hexdigest()


def latest_source_commit(files: list[Path]) -> str:
    paths = [rel(path) for path in files]
    return run_git("log", "-1", "--format=%H", "--", *paths)


def package_manifests() -> dict[str, Path]:
    manifests: dict[str, Path] = {}
    candidates = [
        ROOT / "Cargo.toml",
        *sorted((ROOT / "crates").glob("*/Cargo.toml")),
        *sorted((ROOT / "modules").glob("*/Cargo.toml")),
    ]
    for manifest in candidates:
        data = tomllib.loads(manifest.read_text(encoding="utf-8"))
        package = data.get("package", {})
        name = package.get("name")
        if isinstance(name, str):
            manifests[name] = manifest
    return manifests


def dependency_graph(manifests: dict[str, Path]) -> dict[str, list[str]]:
    dependents: dict[str, set[str]] = defaultdict(set)
    for consumer, manifest in manifests.items():
        data = tomllib.loads(manifest.read_text(encoding="utf-8"))
        sections = [
            data.get("dependencies", {}),
            data.get("dev-dependencies", {}),
            data.get("build-dependencies", {}),
        ]
        target = data.get("target", {})
        if isinstance(target, dict):
            for target_config in target.values():
                if isinstance(target_config, dict):
                    sections.extend(
                        [
                            target_config.get("dependencies", {}),
                            target_config.get("dev-dependencies", {}),
                            target_config.get("build-dependencies", {}),
                        ]
                    )
        for section in sections:
            if not isinstance(section, dict):
                continue
            for dependency, specification in section.items():
                package_name = dependency
                if isinstance(specification, dict) and isinstance(specification.get("package"), str):
                    package_name = specification["package"]
                if package_name in manifests:
                    dependents[package_name].add(consumer)
    return {key: sorted(value) for key, value in sorted(dependents.items())}


def file_record(path: Path) -> dict[str, object]:
    text = path.read_text(encoding="utf-8")
    public_items = [
        {"kind": kind, "name": name}
        for kind, name in PUB_ITEM_RE.findall(text)
    ]
    marker_counts = {marker: text.lower().count(marker.lower()) for marker in MARKERS}
    marker_counts = {key: value for key, value in marker_counts.items() if value}
    return {
        "path": rel(path),
        "bytes": len(text.encode()),
        "lines": text.count("\n") + 1,
        "sha256": hashlib.sha256(text.encode()).hexdigest(),
        "functions": sorted(set(FN_RE.findall(text))),
        "modules": sorted(set(MODULE_RE.findall(text))),
        "public_items": public_items,
        "marker_counts": marker_counts,
    }


def command_variants(cli_main: Path) -> list[str]:
    text = cli_main.read_text(encoding="utf-8")
    match = COMMAND_ENUM_RE.search(text)
    if not match:
        return []
    return VARIANT_RE.findall(match.group("body"))


def method_literals(files: list[Path]) -> list[str]:
    values: set[str] = set()
    for path in files:
        text = path.read_text(encoding="utf-8")
        for value in STRING_METHOD_RE.findall(text):
            if "." in value and len(value) <= 96:
                values.add(value)
    return sorted(values)


def exact_consumers(token: str, files: list[Path]) -> list[str]:
    consumers = []
    for path in files:
        if token in path.read_text(encoding="utf-8", errors="replace"):
            consumers.append(rel(path))
    return consumers


def main() -> None:
    manifests = package_manifests()
    dependents = dependency_graph(manifests)
    all_cluster_files = sorted(
        {path for paths in CLUSTERS.values() for path in rust_files(paths)}
    )
    repository_rs = sorted(
        path
        for path in ROOT.rglob("*.rs")
        if ".git" not in path.parts and "target" not in path.parts
    )

    records: dict[str, object] = {}
    for cluster, roots in CLUSTERS.items():
        files = rust_files(roots)
        package = PACKAGE_BY_CLUSTER[cluster]
        records[cluster] = {
            "package": package,
            "files": [file_record(path) for path in files],
            "file_count": len(files),
            "bytes": sum(path.stat().st_size for path in files),
            "lines": sum(
                path.read_text(encoding="utf-8").count("\n") + 1 for path in files
            ),
            "source_sha256": source_digest(files),
            "cargo_dependents": dependents.get(package, []),
            "method_literals": method_literals(files),
        }

    cli_main = ROOT / "crates/swarm-cli/src/main.rs"
    cli_variants = command_variants(cli_main)
    facts = {
        "root_lib_reexports_kernel_host": "pub use swarm_kernel_host::*"
        in (ROOT / "src/lib.rs").read_text(encoding="utf-8"),
        "root_main_spawns_kernel_host_sibling": "swarm-kernel-host"
        in (ROOT / "src/main.rs").read_text(encoding="utf-8"),
        "cli_delegates_host": "delegate_to_host" in cli_main.read_text(encoding="utf-8"),
        "cli_delegates_mcp": "delegate_to_mcp" in cli_main.read_text(encoding="utf-8"),
        "cli_command_variants": cli_variants,
        "cli_command_variant_count": len(cli_variants),
        "kernel_consumers": exact_consumers("swarm_kernel::", repository_rs),
        "bus_consumers": exact_consumers("swarm_bus::", repository_rs),
        "telemetry_consumers": exact_consumers("swarm_telemetry::", repository_rs),
        "legacy_worker_coordinator_present": (
            ROOT / "crates/swarm-kernel-host/src/host/legacy_optional_workers.rs"
        ).exists(),
        "legacy_worker_demand_present": (
            ROOT / "crates/swarm-kernel-host/src/store/legacy_worker_demand.rs"
        ).exists(),
        "bus_dispatcher_binary_present": (
            ROOT / "crates/swarm-bus/src/bin/dispatcher.rs"
        ).exists(),
    }

    touching_commit = latest_source_commit(all_cluster_files)
    inventory = {
        "schema_version": 2,
        "latest_source_touching_commit": touching_commit,
        "cluster_source_sha256": source_digest(all_cluster_files),
        "clusters": records,
        "facts": facts,
    }
    OUT_JSON.write_text(json.dumps(inventory, indent=2) + "\n", encoding="utf-8")

    lines = [
        "# Entrypoint and public-surface source inventory",
        "",
        f"Latest commit touching an inspected source file: `{touching_commit}`.",
        f"Combined inspected-cluster source SHA-256: `{inventory['cluster_source_sha256']}`.",
        "",
        "This file records syntax-level facts only. It does not prove authorization, runtime behavior, absence of hidden consumers or qualification.",
        "",
        "## Cluster size and package consumers",
        "",
        "| Cluster | Rust files | Lines | Bytes | Cargo dependents |",
        "| --- | ---: | ---: | ---: | --- |",
    ]
    for cluster, record in records.items():
        deps = ", ".join(f"`{item}`" for item in record["cargo_dependents"]) or "none"
        lines.append(
            f"| `{cluster}` | {record['file_count']} | {record['lines']} | {record['bytes']} | {deps} |"
        )

    lines.extend(
        [
            "",
            "## Entrypoint facts",
            "",
            f"- Root library re-exports `swarm-kernel-host`: `{facts['root_lib_reexports_kernel_host']}`.",
            f"- Root binary locates a sibling `swarm-kernel-host`: `{facts['root_main_spawns_kernel_host_sibling']}`.",
            f"- Public CLI has explicit host delegation: `{facts['cli_delegates_host']}`.",
            f"- Public CLI has explicit MCP delegation: `{facts['cli_delegates_mcp']}`.",
            f"- Parsed top-level CLI command variants: {facts['cli_command_variant_count']}.",
            "",
            "### CLI command variants",
            "",
            ", ".join(f"`{item}`" for item in cli_variants) or "None parsed.",
            "",
            "## Exact Rust token consumers",
            "",
        ]
    )
    for label, key in [
        ("`swarm_kernel::`", "kernel_consumers"),
        ("`swarm_bus::`", "bus_consumers"),
        ("`swarm_telemetry::`", "telemetry_consumers"),
    ]:
        lines.append(f"### {label}")
        lines.append("")
        consumers = facts[key]
        lines.extend(f"- `{item}`" for item in consumers)
        if not consumers:
            lines.append("- None detected.")
        lines.append("")

    lines.extend(
        [
            "## File inventory",
            "",
            "| Cluster | File | Lines | Public items | Markers |",
            "| --- | --- | ---: | ---: | --- |",
        ]
    )
    for cluster, record in records.items():
        for item in record["files"]:
            markers = ", ".join(
                f"{key}={value}" for key, value in item["marker_counts"].items()
            ) or "none"
            lines.append(
                f"| `{cluster}` | `{item['path']}` | {item['lines']} | {len(item['public_items'])} | {markers} |"
            )

    lines.extend(
        [
            "",
            "Machine-readable inventory: [`source-surface-inventory.json`](source-surface-inventory.json).",
            "",
        ]
    )
    OUT_MD.write_text("\n".join(lines), encoding="utf-8")


if __name__ == "__main__":
    main()
