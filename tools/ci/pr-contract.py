#!/usr/bin/env python3
"""Read-only PR ownership check. Authority comes only from the checked-out base."""

import argparse
import json
import os
from pathlib import Path
import re
import sys
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

MARKER = "<!-- eliot-contract -->"
BLOCK = re.compile(r"<!-- eliot-contract -->\s*```json\s*\n(.*?)\n```", re.S)
SHA = re.compile(r"[0-9a-f]{40}\Z")
ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,79}\Z")
STATUSES = {"passed", "failed", "blocked", "not_run"}
PATH_KEYS = ("owned_files", "owned_prefixes", "forbidden_files", "forbidden_prefixes")


class Invalid(ValueError):
    pass


def require(ok, message):
    if not ok:
        raise Invalid(message)


def strict_json(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, "duplicate JSON key")
            result[key] = value
        return result

    def constant(_):
        raise Invalid("non-finite JSON number")

    return json.loads(raw, object_pairs_hook=pairs, parse_constant=constant)


def text(value, label):
    require(isinstance(value, str) and 0 < len(value.strip()) <= 2000,
            f"missing or oversized {label}")
    return value


def positive(value, label):
    require(type(value) is int and value > 0, f"invalid {label}")
    return value


def paths(value, label):
    require(isinstance(value, list) and len(value) <= 200, f"invalid {label}")
    require(len(value) == len(set(value)) if all(isinstance(p, str) for p in value)
            else False, f"duplicate or non-string {label}")
    for path in value:
        require(path and path == path.strip() and not path.startswith("/") and "//" not in path
                and not re.search(r"[\\:*?\[\]\x00-\x1f\x7f]", path)
                and all(part not in {"", ".", ".."} for part in path.rstrip("/").split("/")),
                f"invalid repository path in {label}")
        require(not label.endswith("files") or not path.endswith("/"),
                f"directory used as exact file in {label}")
    return value


def contains(path, files, prefixes):
    return path in files or any(path.startswith(prefix) for prefix in prefixes)


def overlap(left, right):
    lf, lp = left
    rf, rp = right
    return (any(contains(p, rf, rp) for p in lf)
            or any(contains(p, lf, lp) for p in rf)
            or any(a.startswith(b) or b.startswith(a) for a in lp for b in rp))


def parse_contract(body):
    require(isinstance(body, str) and len(body) <= 65536, "missing or oversized PR body")
    blocks = BLOCK.findall(body)
    require(body.count(MARKER) == 1 and len(blocks) == 1,
            "expected exactly one eliot-contract fenced JSON block")
    contract = strict_json(blocks[0])
    required = {"issue", "slice", "manager", "track", "base_sha", "head_sha",
                "reviewed_head_sha", "depends_on_prs", "owner_decision_issue",
                "scope_exception", "connected_edge", "replaced_or_deleted_responsibility",
                "gates", "tests", "native", "load", "residual_uncertainty", "rollback",
                "reviews", *PATH_KEYS}
    require(isinstance(contract, dict) and set(contract) == required,
            "contract has missing or unknown fields")
    positive(contract["issue"], "owning Issue")
    for key in ("slice", "manager", "track"):
        require(isinstance(contract[key], str) and ID.fullmatch(contract[key]), f"invalid {key}")
    for key in ("base_sha", "head_sha"):
        require(isinstance(contract[key], str) and SHA.fullmatch(contract[key]), f"invalid {key}")
    reviewed = contract["reviewed_head_sha"]
    require(isinstance(reviewed, str) and (reviewed == "not_reviewed" or SHA.fullmatch(reviewed)),
            "invalid reviewed_head_sha")
    for key in PATH_KEYS:
        paths(contract[key], key)
    require(contract["owned_files"] or contract["owned_prefixes"], "empty requested ownership")
    require(not overlap((contract["owned_files"], contract["owned_prefixes"]),
                        (contract["forbidden_files"], contract["forbidden_prefixes"])),
            "contradictory owned and forbidden declarations")
    deps = contract["depends_on_prs"]
    require(isinstance(deps, list) and len(deps) <= 20, "invalid depends_on_prs")
    for dep in deps:
        positive(dep, "dependency PR")
    require(len(deps) == len(set(deps)), "duplicate dependency PR")
    decision = contract["owner_decision_issue"]
    if decision is not None:
        positive(decision, "owner decision Issue")
    exception = contract["scope_exception"]
    if exception is not None:
        text(exception, "scope_exception")
        require(decision is not None, "scope exception lacks owner decision Issue")
    for key in ("connected_edge", "replaced_or_deleted_responsibility", "residual_uncertainty", "rollback"):
        text(contract[key], key)
    for key in ("tests", "native", "load"):
        require(contract[key] in STATUSES, f"missing explicit {key} disposition")
    gates = contract["gates"]
    require(isinstance(gates, dict) and set(gates) == {"rustfmt", "clippy", "syntax"},
            "invalid source gates")
    require(all(isinstance(v, str) and v in STATUSES for v in gates.values()), "invalid gate disposition")
    require(isinstance(contract["reviews"], list) and len(contract["reviews"]) <= 10,
            "invalid reviews")
    return contract


def load_policy(root):
    path = root / ".github/eliot-change-policy.json"
    require(path.is_file() and not path.is_symlink(),
            "base policy absent: owner-reviewed bootstrap required; candidate policy is not authority")
    policy = strict_json(path.read_text(encoding="utf-8"))
    require(isinstance(policy, dict) and policy.get("version") == 1, "unsupported base policy")
    require(isinstance(policy.get("tracks"), dict), "invalid base tracks")
    for name, track in policy["tracks"].items():
        require(ID.fullmatch(name) and isinstance(track, dict), "invalid policy track")
        require(type(track.get("enabled")) is bool and track.get("max_active_prs") == 1,
                "invalid track activation/WIP policy")
        for key in PATH_KEYS:
            paths(track.get(key), key)
        require(not overlap((track["owned_files"], track["owned_prefixes"]),
                            (track["forbidden_files"], track["forbidden_prefixes"])),
                "contradictory base track policy")
        require(isinstance(track.get("issues"), list), "missing policy Issue allowlist")
        for issue in track["issues"]:
            positive(issue, "policy Issue")
        require(isinstance(track.get("decision_issues"), list), "missing owner decision allowlist")
        for issue in track["decision_issues"]:
            positive(issue, "policy decision Issue")
        require(type(track.get("decision_required")) is bool, "missing decision policy")
    for rule in policy.get("restricted", []):
        paths(rule.get("files"), "restricted_files")
        paths(rule.get("prefixes"), "restricted_prefixes")
        require(isinstance(rule.get("tracks"), list)
                and all(track in policy["tracks"] for track in rule["tracks"]), "invalid restricted tracks")
    # No generated exclusions are authorized by this bootstrap policy. Add only with source/generator proof.
    require(policy.get("generated") == [], "unsupported generated exclusion policy")
    return policy


def authorize(contract, policy, pr):
    require(contract["base_sha"] == pr["base"]["sha"], "base SHA mismatch")
    require(contract["head_sha"] == pr["head"]["sha"], "head SHA mismatch")
    track = policy["tracks"].get(contract["track"])
    require(track is not None and track["enabled"], "track not enabled by base policy")
    require(contract["issue"] in track["issues"], "owning Issue not authorized for track")
    if track["decision_required"]:
        require(contract["owner_decision_issue"] is not None, "track requires owner decision Issue")
    if contract["owner_decision_issue"] is not None:
        require(contract["owner_decision_issue"] in track["decision_issues"],
                "owner decision Issue not authorized by base track")
    for path in contract["owned_files"]:
        require(contains(path, track["owned_files"], track["owned_prefixes"]),
                "requested file exceeds base track scope")
    for prefix in contract["owned_prefixes"]:
        require(any(prefix.startswith(allowed) for allowed in track["owned_prefixes"]),
                "requested prefix exceeds base track scope")
    require(not overlap((contract["owned_files"], contract["owned_prefixes"]),
                        (track["forbidden_files"], track["forbidden_prefixes"])),
            "request overlaps base-forbidden paths")
    for rule in policy["restricted"]:
        if overlap((contract["owned_files"], contract["owned_prefixes"]),
                   (rule["files"], rule["prefixes"])):
            require(contract["track"] in rule["tracks"]
                    and contract["owner_decision_issue"] is not None,
                    "sensitive/shared path needs dedicated track and owner decision")
    return track


class Github:
    def __init__(self, repository):
        require(re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository), "invalid repository")
        self.repository = repository
        self.owner = repository.split("/")[0]
        self.base = os.environ.get("GITHUB_API_URL", "https://api.github.com").rstrip("/")
        require(self.base.startswith("https://"), "GitHub API must use HTTPS")
        self.token = os.environ.get("GH_TOKEN", "")
        require(self.token, "read-only GitHub token missing")
        self.requests = 0

    def get(self, suffix):
        self.requests += 1
        require(self.requests <= 80, "bounded API request budget exhausted")
        request = Request(f"{self.base}/repos/{self.repository}/{suffix}", headers={
            "Authorization": f"Bearer {self.token}", "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "eliot-governance-guard"})
        try:
            with urlopen(request, timeout=20) as response:
                raw = response.read(8 * 1024 * 1024 + 1)
                require(len(raw) <= 8 * 1024 * 1024, "oversized GitHub API response")
                return strict_json(raw)
        except HTTPError as error:
            raise Invalid(f"GitHub read failed (HTTP {error.code}); coverage incomplete") from None
        except (URLError, TimeoutError, OSError):
            raise Invalid("GitHub read unavailable; coverage incomplete") from None

    def pages(self, suffix, max_pages):
        items = []
        for page in range(1, max_pages + 1):
            batch = self.get(f"{suffix}{'&' if '?' in suffix else '?'}per_page=100&page={page}")
            require(isinstance(batch, list), "invalid paginated response")
            items.extend(batch)
            if len(batch) < 100:
                return items
        raise Invalid("API pagination bound reached; coverage incomplete")

    def issue(self, number, decision=False):
        issue = self.get(f"issues/{number}")
        require(issue.get("state") == "open" and "pull_request" not in issue,
                "owning/decision Issue must be open in this repository")
        if decision:
            require(issue.get("user", {}).get("login", "").casefold() == self.owner.casefold(),
                    "decision Issue must be authored by repository owner")


def review(contract, changed):
    require(contract["reviewed_head_sha"] == contract["head_sha"], "current head not reviewed")
    covered = set()
    require(contract["reviews"], "missing independent exact-SHA review record")
    for record in contract["reviews"]:
        require(isinstance(record, dict) and set(record) == {
            "reviewer_role", "reviewed_head_sha", "reviewed_paths", "findings", "disposition"},
            "invalid review record")
        role = record["reviewer_role"]
        require(isinstance(role, str) and ID.fullmatch(role) and role.startswith("independent-"),
                "review role must identify an independent reviewer")
        require(record["reviewed_head_sha"] == contract["head_sha"], "stale review SHA")
        covered.update(paths(record["reviewed_paths"], "reviewed_files"))
        require(record["disposition"] == "approved" and record["findings"] == [],
                "review contains unresolved findings or lacks approval")
    require(changed <= covered, "review record does not cover every changed/renamed path")


def collisions(api, contract, policy, current):
    pulls = api.pages("pulls?state=open&sort=created&direction=asc", 6)
    claims = [(current["created_at"], current["number"])]
    require(len({pr["number"] for pr in pulls}) == len(pulls), "unstable PR pagination")
    require(any(pr["number"] == current["number"] for pr in pulls), "current PR absent from open inventory")
    for other in pulls:
        if other["number"] == current["number"]:
            continue
        body = other.get("body") or ""
        if other.get("draft") and MARKER not in body:
            continue  # Legacy research handoffs only; #111 owns reconciliation.
        require(MARKER in body, "open non-research PR lacks ownership contract; coverage incomplete")
        candidate = parse_contract(body)
        authorize(candidate, policy, other)
        conflict = (candidate["track"] == contract["track"]
                    or candidate["manager"] == contract["manager"]
                    or overlap((candidate["owned_files"], candidate["owned_prefixes"]),
                               (contract["owned_files"], contract["owned_prefixes"])))
        if conflict:
            claims.append((other["created_at"], other["number"]))
    def snapshot(rows):
        return sorted((p["number"], p["head"]["sha"], p["base"]["sha"],
                       p.get("body") or "", p.get("draft")) for p in rows)
    require(snapshot(pulls) == snapshot(api.pages("pulls?state=open&sort=created&direction=asc", 6)),
            "open PR inventory changed during collision check")
    # Preserve oldest ownership, but never green either colliding candidate: a read-only
    # job cannot revoke a sibling's cached success after an older claim is expanded.
    require(len(claims) == 1,
            f"overlapping claims: owner PR #{min(claims)[1]}; release other claims before approval")


def validate(event, root, repository):
    require("pull_request" in event, "validator runs only for pull requests")
    pr = event["pull_request"]
    api = Github(repository)
    number = positive(pr["number"], "PR number")
    current = api.get(f"pulls/{number}")
    require(current.get("state") == "open" and current["base"]["ref"] == "main", "PR must target open main")
    require(current["head"]["sha"] == pr["head"]["sha"]
            and current["base"]["sha"] == pr["base"]["sha"]
            and current.get("body") == pr.get("body"), "stale PR event; rerun current head/body")
    policy = load_policy(root)
    contract = parse_contract(pr.get("body"))
    track = authorize(contract, policy, pr)
    api.issue(contract["issue"])
    if contract["owner_decision_issue"] is not None:
        api.issue(contract["owner_decision_issue"], decision=True)
    require(number not in contract["depends_on_prs"], "PR depends on itself")
    for dep in contract["depends_on_prs"]:
        dependency = api.get(f"pulls/{dep}")
        require(dependency.get("merged_at"), "dependency not merged; wait and rebase")
        comparison = api.get(f"compare/{dependency['merge_commit_sha']}...{pr['base']['sha']}")
        require(comparison.get("status") in {"ahead", "identical"},
                "dependency merge absent from declared base; wait and rebase")
    files = api.pages(f"pulls/{number}/files", 30)
    require(0 < len(files) == current["changed_files"], "changed-file coverage incomplete")
    changed = set()
    for file in files:
        names = [file["filename"]]
        if file.get("previous_filename"):
            names.append(file["previous_filename"])
        paths(names, "changed_files")
        for path in names:
            require(path not in changed, "duplicate changed-file response")
            changed.add(path)
            require(contains(path, contract["owned_files"], contract["owned_prefixes"]),
                    "changed/renamed path undeclared")
            require(not contains(path, contract["forbidden_files"], contract["forbidden_prefixes"])
                    and not contains(path, track["forbidden_files"], track["forbidden_prefixes"]),
                    "changed/renamed path forbidden")
    production = [f for f in files if f["filename"].startswith(("crates/", "src/", "modules/", "migrations/"))]
    migrations = [f for f in files if f["filename"].startswith("crates/swarm-kernel-host/migrations/")]
    if (len(production) > 12 or sum(f["additions"] + f["deletions"] for f in files) > 1500
            or len(migrations) > 1):
        require(contract["scope_exception"] is not None, "broad diff needs owner-linked scope_exception")
    require(contract["gates"]["syntax"] == "passed", "syntax/parsing gate not passed")
    if any(path.endswith(".rs") for path in changed):
        require(contract["gates"]["rustfmt"] == "passed" and contract["gates"]["clippy"] == "passed",
                "Rust source requires scoped rustfmt and Clippy evidence")
    review(contract, changed)
    collisions(api, contract, policy, current)
    # Detect body/head/base drift while the bounded inventory was being read.
    latest = api.get(f"pulls/{number}")
    require(latest["head"]["sha"] == pr["head"]["sha"]
            and latest["base"]["sha"] == pr["base"]["sha"]
            and latest.get("body") == pr.get("body"), "PR changed during validation")
    print(f"PR contract passed: #{number}; track={contract['track']}; files={len(files)}; head={contract['head_sha']}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event", type=Path, required=True)
    parser.add_argument("--policy-root", type=Path, required=True)
    parser.add_argument("--repository", required=True)
    args = parser.parse_args()
    try:
        validate(strict_json(args.event.read_text(encoding="utf-8")), args.policy_root, args.repository)
    except (Invalid, KeyError, TypeError, json.JSONDecodeError, OSError,
            UnicodeError, RecursionError, OverflowError) as error:
        # Never print API bodies, submitted PR fields, credentials or machine paths.
        message = str(error) if isinstance(error, Invalid) else "invalid metadata/schema or unavailable input"
        print(f"PR contract failed: {message}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
