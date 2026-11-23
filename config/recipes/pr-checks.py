"""Read-only, bounded GitHub pull request watch for an Eliot project timer."""

import hashlib
import json
import os
import re
import sys
import tempfile
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


POLL_SECONDS = 60
MAX_BACKOFF_SECONDS = 15 * 60
WATCH_SECONDS = 6 * 60 * 60
MAX_RESPONSE_BYTES = 2 * 1024 * 1024


def github_get(path):
    request = Request(
        f"https://api.github.com/repos/{path}",
        headers={
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
    head, state = pull["head"]["sha"], pull["state"]
    if pull["number"] != number or not re.fullmatch(r"[0-9a-f]{40}", head):
        raise ValueError("GitHub returned an unexpected PR or head")
    if state not in {"open", "closed"} or not isinstance(pull["merged"], bool):
        raise ValueError("GitHub returned an unknown PR state")
    return head, "merged" if pull["merged"] else state


def check_result(repository, head):
    base = f"{repository}/commits/{head}"
    runs = github_get(f"{base}/check-runs?filter=latest&per_page=100")
    statuses = github_get(f"{base}/status?per_page=100")
    outcomes = []
    for payload, key, name in (
        (runs, "check_runs", "name"),
        (statuses, "statuses", "context"),
    ):
        rows = payload[key]
        if len(rows) != payload["total_count"]:
            raise ValueError(f"incomplete {key} read (at most 100 supported)")
        for row in rows:
            conclusion = (
                row["conclusion"] if row["status"] == "completed" else "pending"
            ) if key == "check_runs" else row["state"]
            outcomes.append((row[name], conclusion or "pending"))

    if not outcomes:
        return "no checks reported", []
    pending = any(conclusion == "pending" for _, conclusion in outcomes)
    failed = [
        (name, conclusion)
        for name, conclusion in outcomes
        if conclusion not in {"success", "neutral", "skipped", "pending"}
    ]
    if pending:
        return "checks still pending", failed
    return ("checks failed" if failed else "checks completed successfully"), failed


def result_event(repository, number, head, pr_state, result, failed):
    url = f"https://github.com/{repository}/pull/{number}"
    summary = f"PR #{number} is {pr_state}. Head {head}: {result}. {url}"
    for name, conclusion in failed[:20]:
        summary += f"\n- {json.dumps(name[:160])}: {json.dumps(conclusion[:32])}"
    if len(failed) > 20:
        summary += f"\n{len(failed) - 20} additional unsuccessful checks."
    key = hashlib.sha256(f"{repository.lower()}#{number}@{head}".encode()).hexdigest()
    return {
        "action": "emit",
        "topic": "pr_checks_result",
        "key": key,
        "data": {
            "repository": repository,
            "pull_number": str(number),
            "head": head,
            "pr_state": pr_state,
            "result": result,
            "summary": summary,
        },
    }


def save_terminal_report(path, report):
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        dir=path.parent, mode="w", encoding="utf-8", delete=False
    ) as output:
        pending = Path(output.name)
        try:
            json.dump(report, output)
        except BaseException:
            output.close()
            pending.unlink()
            raise
    try:
        os.replace(pending, path)
    finally:
        pending.unlink(missing_ok=True)


def observe(repository, number):
    head, pr_state = pr_snapshot(repository, number)
    try:
        result, failed = check_result(repository, head)
    except (URLError, ValueError, TimeoutError, OSError, KeyError) as error:
        if pr_state == "open":
            raise
        print(f"pr-checks: final checks unavailable ({error})", file=sys.stderr)
        result, failed = "check result unavailable", []
    if pr_state == "open" and result in {"no checks reported", "checks still pending"}:
        return None
    if (head, pr_state) != pr_snapshot(repository, number):
        return None
    return result_event(repository, number, head, pr_state, result, failed)


def watch(repository, number, path):
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))

    deadline = time.monotonic() + WATCH_SECONDS
    delay = POLL_SECONDS
    while time.monotonic() < deadline:
        try:
            report = observe(repository, number)
        except (URLError, ValueError, TimeoutError, OSError, KeyError) as error:
            wait = delay
            if isinstance(error, HTTPError) and error.headers.get("X-RateLimit-Remaining") == "0":
                reset = error.headers.get("X-RateLimit-Reset", "")
                if reset.isdecimal():
                    wait = max(wait, int(reset) - int(time.time()))
            print(f"pr-checks: GitHub read deferred ({error})", file=sys.stderr)
        else:
            if report is not None:
                if report["data"]["pr_state"] != "open":
                    save_terminal_report(path, report)
                return report
            wait = delay
        time.sleep(max(0, min(wait, deadline - time.monotonic())))
        delay = min(2 * delay, MAX_BACKOFF_SECONDS)
    print("pr-checks: watch expired before a result was observed", file=sys.stderr)
    return None


def main():
    try:
        _, repository, number = sys.argv
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*/[A-Za-z0-9._-]+", repository):
            raise ValueError("repository must be OWNER/REPO")
        if repository.split("/")[1] in {".", ".."} or not number.isascii() or not number.isdecimal():
            raise ValueError("expected a repository and positive PR number")
        number = int(number)
        if number < 1:
            raise ValueError("expected a positive PR number")
        json.load(sys.stdin)
        key = hashlib.sha256(f"{repository.lower()}#{number}".encode()).hexdigest()
        path = Path(".eliot/state/pr-checks") / f"{key}.json"
        report = watch(repository, number, path)
        if report:
            print(json.dumps(report, separators=(",", ":")))
    except (KeyError, TypeError, ValueError, OSError) as error:
        print(f"pr-checks: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
