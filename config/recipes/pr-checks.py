"""Read-only, bounded GitHub pull request watch for an Eliot timer."""

import argparse
import hashlib
import json
import os
import re
import sys
import tempfile
import time
import unicodedata
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

from github_ci import github_request


POLL_SECONDS = 60
MAX_BACKOFF_SECONDS = 15 * 60
WATCH_SECONDS = 6 * 60 * 60
MAX_RESPONSE_BYTES = 2 * 1024 * 1024
READ_ERRORS = (URLError, OSError, ValueError, KeyError, TypeError)
PENDING_RESULTS = {"no checks reported", "checks still pending"}


class CheckListLimit(ValueError):
    """The watch cannot verify a complete check result from one page."""


def github_get(path):
    request = github_request(
        f"https://api.github.com/repos/{path}",
        {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "eliot-pr-checks-recipe",
        },
    )
    with urlopen(request, timeout=7) as response:
        data = response.read(MAX_RESPONSE_BYTES + 1)
    if len(data) > MAX_RESPONSE_BYTES:
        raise ValueError("GitHub response exceeds the 2 MiB limit")
    value = json.loads(data)
    if not isinstance(value, dict):
        raise ValueError("GitHub returned an unexpected response")
    return value


def pr_snapshot(repository, number):
    pull = github_get(f"{repository}/pulls/{number}")
    head, state, updated_at = pull["head"]["sha"], pull["state"], pull["updated_at"]
    if pull["number"] != number or not re.fullmatch(r"[0-9a-f]{40}", head):
        raise ValueError("GitHub returned an unexpected PR or head")
    if state not in {"open", "closed"} or not isinstance(pull["merged"], bool):
        raise ValueError("GitHub returned an unknown PR state")
    if not isinstance(updated_at, str) or not updated_at:
        raise ValueError("GitHub omitted the PR update time")
    return head, "merged" if pull["merged"] else state, updated_at


def clean_name(value):
    if not isinstance(value, str):
        raise ValueError("GitHub returned an invalid check name")
    return "".join(ch for ch in value if unicodedata.category(ch) not in {"Cc", "Cf", "Cs"})


def check_result(repository, head):
    base = f"{repository}/commits/{head}"
    outcomes = []
    for endpoint, key, label in (
        ("check-runs?filter=latest&per_page=100", "check_runs", "name"),
        ("status?per_page=100", "statuses", "context"),
    ):
        payload = github_get(f"{base}/{endpoint}")
        rows, total = payload[key], payload["total_count"]
        if not isinstance(rows, list) or type(total) is not int or total < 0:
            raise ValueError(f"GitHub returned an invalid {key} list")
        if total > 100 or len(rows) > 100:
            raise CheckListLimit(f"{key} exceed the 100-item limit")
        if len(rows) != total:
            raise ValueError(f"GitHub returned an incomplete {key} list")
        for row in rows:
            if key == "check_runs":
                conclusion = row["conclusion"] if row["status"] == "completed" else "pending"
            else:
                conclusion = row["state"]
            outcomes.append((clean_name(row[label]), conclusion or "pending"))

    if not outcomes:
        return "no checks reported", []
    failed = [
        (name, conclusion)
        for name, conclusion in outcomes
        if conclusion not in {"success", "neutral", "skipped", "pending"}
    ]
    if any(conclusion == "pending" for _, conclusion in outcomes):
        return "checks still pending", failed
    return ("checks failed" if failed else "checks completed successfully"), failed


def result_event(repository, number, snapshot, result, failed=(), kind="completed"):
    head, state = snapshot[:2] if snapshot else (None, "unobserved")
    url = f"https://github.com/{repository}/pull/{number}"
    summary = f"PR #{number} last observed state {state}. Head {head or 'unobserved'}: {result}. {url}"
    for name, conclusion in failed[:20]:
        summary += f"\n- {json.dumps(name[:160])}: {json.dumps(conclusion[:32])}"
    if len(failed) > 20:
        summary += f"\n{len(failed) - 20} additional unsuccessful checks."
    identity = f"{repository.lower()}#{number}@{head or 'unobserved'}"
    if kind != "completed":
        identity += f":{kind}"
    key = hashlib.sha256(identity.encode()).hexdigest()
    return {
        "action": "emit",
        "topic": "pr_checks_result",
        "key": key,
        "data": {
            "repository": repository,
            "pull_number": str(number),
            "head": head,
            "pr_state": state,
            "result": result,
            "result_kind": kind,
            "summary": summary,
        },
    }


def cached_terminal(path, snapshot):
    if snapshot[1] == "open":
        path.unlink(missing_ok=True)
        return None
    try:
        with path.open("rb") as source:
            raw = source.read(32 * 1024 + 1)
        if len(raw) <= 32 * 1024:
            saved = json.loads(raw)
            report = saved["report"]
            if saved["snapshot"] == list(snapshot) and report["data"]["head"] == snapshot[0]:
                return report
    except (OSError, KeyError, TypeError, ValueError):
        pass
    return None


def save_terminal(path, snapshot, report):
    path.parent.mkdir(parents=True, exist_ok=True)
    pending = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=path.parent, mode="w", encoding="utf-8", delete=False
        ) as output:
            pending = Path(output.name)
            json.dump({"snapshot": snapshot, "report": report}, output)
        os.replace(pending, path)
    finally:
        if pending is not None:
            pending.unlink(missing_ok=True)


def observe_checks(repository, snapshot):
    try:
        result, failed = check_result(repository, snapshot[0])
        return result, failed, "completed"
    except CheckListLimit as error:
        return f"check result unavailable because {error}", [], "check-limit"
    except READ_ERRORS:
        if snapshot[1] == "open":
            raise
        return "check result unavailable", [], "unavailable"


def watch(repository, number, directory, seconds):
    deadline = time.monotonic() + seconds
    delay = POLL_SECONDS
    last = None
    last_checks = None
    while True:
        wait = delay
        try:
            snapshot = pr_snapshot(repository, number)
            if last != snapshot:
                last_checks = None
            last = snapshot
            if time.monotonic() >= deadline:
                break
            path = directory / f"{number}-{snapshot[0]}.json"
            cached = cached_terminal(path, snapshot)
            if cached is not None:
                return cached
            result, failed, kind = observe_checks(repository, snapshot)
            if time.monotonic() < deadline:
                last_checks = result
                if snapshot[1] != "open" or result not in PENDING_RESULTS:
                    current = pr_snapshot(repository, number)
                    if current == snapshot and time.monotonic() < deadline:
                        report = result_event(repository, number, snapshot, result, failed, kind)
                        if kind == "completed" and snapshot[1] != "open":
                            save_terminal(path, snapshot, report)
                        return report
                    last = current
                    if current != snapshot:
                        last_checks = None
        except READ_ERRORS as error:
            if isinstance(error, HTTPError) and error.headers.get("X-RateLimit-Remaining") == "0":
                reset = error.headers.get("X-RateLimit-Reset", "")
                if reset.isdecimal():
                    wait = max(wait, int(reset) - int(time.time()))
            print(f"pr-checks: GitHub read deferred ({error})", file=sys.stderr)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(wait, remaining))
        if time.monotonic() >= deadline:
            break
        delay = min(2 * delay, MAX_BACKOFF_SECONDS)

    summary = "no checks finished within the watch window"
    if last_checks is not None:
        summary += f" (last observed check result: {last_checks})"
    return result_event(repository, number, last, summary, kind="expiry")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("repository")
    parser.add_argument("number")
    parser.add_argument("--watch-seconds", type=float, default=WATCH_SECONDS)
    args = parser.parse_args()
    try:
        repository, number = args.repository, args.number
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*/[A-Za-z0-9._-]+", repository):
            raise ValueError("repository must be OWNER/REPO")
        if repository.split("/")[1] in {".", ".."} or not number.isascii() or not number.isdecimal():
            raise ValueError("expected a repository and positive PR number")
        number = int(number)
        if number < 1:
            raise ValueError("expected a positive PR number")
        if not 0 < args.watch_seconds <= WATCH_SECONDS:
            raise ValueError(f"--watch-seconds must be positive and at most {WATCH_SECONDS}")
        json.load(sys.stdin)
        repo_key = hashlib.sha256(repository.lower().encode()).hexdigest()
        directory = Path(".eliot/state/pr-checks") / repo_key
        report = watch(repository, number, directory, args.watch_seconds)
        print(json.dumps(report, separators=(",", ":")))
    except (KeyError, TypeError, ValueError, OSError) as error:
        print(f"pr-checks: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
