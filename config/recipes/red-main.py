#!/usr/bin/env python3
"""Poll bounded recent GitHub Actions failures on main and emit rule facts."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import sys
import tempfile
import unicodedata
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

from github_ci import github_request

API_ROOT = "https://api.github.com"
LOOKBACK_DAYS = 14
RUNS_PER_PAGE = 100
MAX_RUN_PAGES = 3
MAX_FAILING_RUNS_PER_POLL = 5
MAX_JOB_PAGES = 2
POLL_INTERVAL_SECONDS = 30 * 60
MAX_RESPONSE_BYTES = 4 * 1024 * 1024
MAX_ACTION_RECORD_BYTES = 60 * 1024
MAX_BASELINE_IDS = RUNS_PER_PAGE * MAX_RUN_PAGES
MAX_BASELINE_BYTES = 32 * 1024
MAX_DISPLAY_CHARS = 256
BASELINE_VERSION = 1
USER_AGENT = "eliot-red-main-recipe"

_REPOSITORY_PART = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,99}\Z")
_RUN_ID = re.compile(r"[0-9]{1,20}\Z")


class GitHubApiError(RuntimeError):
    """A bounded public GitHub REST read failed or returned unusable data."""


def validate_repository(value: str) -> str:
    parts = value.split("/")
    if len(parts) != 2 or any(not _REPOSITORY_PART.fullmatch(part) for part in parts):
        raise argparse.ArgumentTypeError("repository must be OWNER/REPOSITORY")
    return value


def validate_run_id(value: str) -> str:
    if not _RUN_ID.fullmatch(value) or int(value) <= 0:
        raise argparse.ArgumentTypeError(
            "run ID must be a positive decimal GitHub Actions ID"
        )
    return str(int(value))


def api_get(path: str, params: dict[str, object] | None = None) -> dict[str, object]:
    url = f"{API_ROOT}{path}"
    if params:
        url = f"{url}?{urlencode(params)}"

    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": USER_AGENT,
    }
    try:
        with urlopen(github_request(url, headers), timeout=15) as response:
            body = response.read(MAX_RESPONSE_BYTES + 1)
    except HTTPError as exc:
        raise GitHubApiError(
            f"GitHub REST returned HTTP {exc.code}; check public API access or API quota"
        ) from None
    except (URLError, TimeoutError, OSError) as exc:
        raise GitHubApiError(f"GitHub REST request failed: {exc}") from None

    if len(body) > MAX_RESPONSE_BYTES:
        raise GitHubApiError("GitHub REST response exceeded the 4 MiB per-response bound")
    try:
        value = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise GitHubApiError("GitHub REST returned invalid JSON") from None
    if not isinstance(value, dict):
        raise GitHubApiError("GitHub REST returned an unexpected JSON shape")
    return value


def parse_timestamp(value: object) -> datetime:
    if not isinstance(value, str):
        raise GitHubApiError("workflow run omitted created_at")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise GitHubApiError("workflow run returned an invalid created_at") from None
    if parsed.tzinfo is None:
        raise GitHubApiError("workflow run returned a timezone-free created_at")
    return parsed.astimezone(timezone.utc)


def run_id_of(run: dict[str, object]) -> str:
    value = run.get("id")
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise GitHubApiError("GitHub returned a workflow run without a valid ID")
    return str(value)


def baseline_path(repository: str, *, replay: bool) -> Path:
    repo_key = hashlib.sha256(repository.lower().encode("utf-8")).hexdigest()
    namespace = "replay" if replay else "production"
    return Path(".eliot/state/red-main") / namespace / f"{repo_key}.json"


def load_baseline(path: Path, repository: str) -> set[str] | None:
    try:
        with path.open("rb") as source:
            raw = source.read(MAX_BASELINE_BYTES + 1)
    except FileNotFoundError:
        return None
    except OSError:
        raise GitHubApiError("could not read the local failed-run baseline") from None

    if len(raw) > MAX_BASELINE_BYTES:
        raise GitHubApiError("local failed-run baseline exceeds its size bound")
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise GitHubApiError("local failed-run baseline is invalid JSON") from None
    if (
        not isinstance(value, dict)
        or type(value.get("version")) is not int
        or value["version"] != BASELINE_VERSION
        or value.get("repository") != repository.lower()
    ):
        raise GitHubApiError("local failed-run baseline has an unsupported shape")

    ids = value.get("failed_run_ids")
    if not isinstance(ids, list) or len(ids) > MAX_BASELINE_IDS:
        raise GitHubApiError("local failed-run baseline exceeds its ID bound")
    normalized: set[str] = set()
    for run_id in ids:
        if (
            not isinstance(run_id, str)
            or not _RUN_ID.fullmatch(run_id)
            or int(run_id) <= 0
            or str(int(run_id)) != run_id
            or run_id in normalized
        ):
            raise GitHubApiError("local failed-run baseline contains an invalid ID")
        normalized.add(run_id)
    return normalized


def save_baseline(path: Path, repository: str, run_ids: set[str]) -> bool:
    if len(run_ids) > MAX_BASELINE_IDS:
        raise GitHubApiError("failed-run baseline exceeds its ID bound")
    value = {
        "version": BASELINE_VERSION,
        "repository": repository.lower(),
        "failed_run_ids": sorted(run_ids, key=int),
    }
    pending: Path | None = None
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            mode="wb", dir=path.parent, prefix=f".{path.name}.", delete=False
        ) as output:
            pending = Path(output.name)
            output.write(json.dumps(value, separators=(",", ":")).encode("utf-8"))
            output.flush()
            os.fsync(output.fileno())
        try:
            os.link(pending, path)
        except FileExistsError:
            return False
        if os.name == "posix":
            directory_fd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
    except OSError:
        raise GitHubApiError("could not persist the local failed-run baseline") from None
    finally:
        if pending is not None:
            pending.unlink(missing_ok=True)
    return True


def baseline_for_scan(
    repository: str, failures: list[dict[str, object]], *, replay: bool
) -> tuple[set[str], bool]:
    current_ids = {run_id_of(run) for run in failures}
    if len(current_ids) > MAX_BASELINE_IDS:
        raise GitHubApiError("failed-run scan exceeds its ID bound")
    path = baseline_path(repository, replay=replay)
    baseline = load_baseline(path, repository)
    if baseline is not None:
        return baseline, False
    if save_baseline(path, repository, current_ids):
        return current_ids, True
    baseline = load_baseline(path, repository)
    if baseline is None:
        raise GitHubApiError("failed-run baseline disappeared during initialization")
    return baseline, False


def sanitize_display_text(value: str) -> str:
    safe = "".join(
        char
        for char in value
        if unicodedata.category(char) not in {"Cc", "Cf", "Cs"}
    )
    safe = " ".join(safe.split())
    if len(safe) > MAX_DISPLAY_CHARS:
        safe = safe[: MAX_DISPLAY_CHARS - 1].rstrip() + "…"
    return safe


def recent_failed_runs(repository: str, now: datetime) -> list[dict[str, object]]:
    cutoff = now - timedelta(days=LOOKBACK_DAYS)
    collected: dict[str, dict[str, object]] = {}

    # The Actions API returns runs newest first. The page cap remains a hard
    # bound even when a high-volume repository has more than 300 recent runs.
    for page in range(1, MAX_RUN_PAGES + 1):
        response = api_get(
            f"/repos/{repository}/actions/runs",
            {
                "branch": "main",
                "status": "failure",
                "exclude_pull_requests": "true",
                "per_page": RUNS_PER_PAGE,
                "page": page,
            },
        )
        runs = response.get("workflow_runs")
        if not isinstance(runs, list):
            raise GitHubApiError("GitHub REST omitted workflow_runs")
        if not runs:
            break

        for item in runs:
            if not isinstance(item, dict):
                continue
            created_at = parse_timestamp(item.get("created_at"))
            if created_at < cutoff:
                continue
            if (
                item.get("head_branch") != "main"
                or item.get("status") != "completed"
                or item.get("conclusion") != "failure"
                or item.get("event") in {"pull_request", "pull_request_target"}
            ):
                continue
            collected[run_id_of(item)] = item

        if len(runs) < RUNS_PER_PAGE:
            break

    return sorted(
        collected.values(),
        key=lambda item: (parse_timestamp(item.get("created_at")), run_id_of(item)),
    )


def select_failure_batch(
    failures: list[dict[str, object]], now: datetime
) -> list[dict[str, object]]:
    if len(failures) <= MAX_FAILING_RUNS_PER_POLL:
        return failures
    batch_count = math.ceil(len(failures) / MAX_FAILING_RUNS_PER_POLL)
    time_bucket = int(now.timestamp()) // POLL_INTERVAL_SECONDS
    batch_index = time_bucket % batch_count
    start = batch_index * MAX_FAILING_RUNS_PER_POLL
    return failures[start : start + MAX_FAILING_RUNS_PER_POLL]


def failing_job_names(repository: str, run: dict[str, object]) -> list[str]:
    run_id = run_id_of(run)
    attempt = run.get("run_attempt")
    if type(attempt) is not int or attempt < 1:
        raise GitHubApiError(f"failed run {run_id} omitted its attempt number")
    job_names: list[str] = []
    total_count: int | None = None
    jobs_seen = 0

    for page in range(1, MAX_JOB_PAGES + 1):
        response = api_get(
            f"/repos/{repository}/actions/runs/{run_id}/attempts/{attempt}/jobs",
            {"per_page": RUNS_PER_PAGE, "page": page},
        )
        jobs = response.get("jobs")
        count = response.get("total_count")
        if not isinstance(jobs, list) or not isinstance(count, int):
            raise GitHubApiError(f"GitHub omitted jobs for failed run {run_id}")
        total_count = count
        if total_count > MAX_JOB_PAGES * RUNS_PER_PAGE:
            raise GitHubApiError(
                f"failed run {run_id} has more than "
                f"{MAX_JOB_PAGES * RUNS_PER_PAGE} jobs; refusing an incomplete job list"
            )
        jobs_seen += len(jobs)

        for job in jobs:
            if not isinstance(job, dict) or job.get("conclusion") not in {"failure", "timed_out"}:
                continue
            name = job.get("name")
            if not isinstance(name, str) or not name.strip():
                raise GitHubApiError(f"GitHub returned an unnamed failing job for run {run_id}")
            job_names.append(sanitize_display_text(name) or "unnamed failing job")

        if len(jobs) < RUNS_PER_PAGE or jobs_seen >= total_count:
            break

    if jobs_seen != total_count:
        raise GitHubApiError(f"failed run {run_id} jobs were incomplete")
    return sorted(set(job_names))


def failure_payload(
    run: dict[str, object], repository: str, job_names: list[str]
) -> dict[str, object]:
    run_id = run_id_of(run)
    workflow = run.get("name")
    if isinstance(workflow, str):
        workflow = sanitize_display_text(workflow) or "unnamed workflow"
    else:
        workflow = "unnamed workflow"
    run_url = f"https://github.com/{repository}/actions/runs/{run_id}"
    created_at = run.get("created_at")
    if not isinstance(created_at, str):
        raise GitHubApiError(f"failed run {run_id} omitted created_at")
    created_at = parse_timestamp(created_at).isoformat().replace("+00:00", "Z")

    jobs_text = ", ".join(json.dumps(name, ensure_ascii=False) for name in job_names)
    jobs_text = jobs_text or "no failing job names returned"
    summary = (
        f"Failed main workflow run {run_id} ({workflow}) at {created_at}; "
        f"failing jobs: {jobs_text}. {run_url}"
    )
    payload: dict[str, object] = {
        "repository": repository,
        "branch": "main",
        "run_id": run_id,
        "workflow": workflow,
        "created_at": created_at,
        "run_url": run_url,
        "failing_jobs": job_names,
        "summary": summary,
    }
    record = {"action": "emit", "topic": "ci.red", "data": payload, "key": run_id}
    encoded_record = json.dumps(
        record, ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")
    if len(summary.encode("utf-8")) > 16 * 1024 or len(encoded_record) > MAX_ACTION_RECORD_BYTES:
        raise GitHubApiError(f"failed run {run_id} details exceed the event size bound")
    return record


def pinned_failed_run(repository: str, pinned_run_id: str) -> dict[str, object]:
    run = api_get(f"/repos/{repository}/actions/runs/{pinned_run_id}")
    if run_id_of(run) != pinned_run_id:
        raise GitHubApiError("GitHub returned a different run ID than the pinned ID")
    if (
        run.get("head_branch") != "main"
        or run.get("event") in {"pull_request", "pull_request_target"}
        or run.get("status") != "completed"
        or run.get("conclusion") != "failure"
    ):
        raise GitHubApiError("pinned run must be a completed failed non-PR run on main")
    return run


def write_record(record: dict[str, object]) -> None:
    serialized = json.dumps(record, separators=(",", ":"))
    sys.stdout.write(serialized + "\n")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=validate_repository, required=True)
    parser.add_argument(
        "--pinned-run-id",
        type=validate_run_id,
        help="read-only replay; seed the isolated baseline, then re-emit unseen pinned runs",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        if args.pinned_run_id:
            run = pinned_failed_run(args.repo, args.pinned_run_id)
            baseline, initialized = baseline_for_scan(args.repo, [run], replay=True)
            if initialized:
                print(
                    "red-main: initialized isolated replay baseline; no event emitted",
                    file=sys.stderr,
                )
                return 0
            if run_id_of(run) in baseline:
                return 0
            jobs = failing_job_names(args.repo, run)
            write_record(failure_payload(run, args.repo, jobs))
            return 0

        now = datetime.now(timezone.utc)
        failures = recent_failed_runs(args.repo, now)
        baseline, initialized = baseline_for_scan(args.repo, failures, replay=False)
        if initialized:
            print(
                f"red-main: initialized baseline with {len(baseline)} current failed run IDs; no event emitted",
                file=sys.stderr,
            )
            return 0

        new_failures = [run for run in failures if run_id_of(run) not in baseline]
        for run in select_failure_batch(new_failures, now):
            try:
                jobs = failing_job_names(args.repo, run)
                write_record(failure_payload(run, args.repo, jobs))
            except GitHubApiError as exc:
                print(f"red-main: {exc}", file=sys.stderr)
        return 0
    except GitHubApiError as exc:
        print(f"red-main: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
