"""Read-only GitHub authentication for the CI recipes."""

import os
import re
import shutil
import subprocess
from functools import cache
from urllib.request import Request


@cache
def github_token():
    for name in ("GITHUB_TOKEN", "GH_TOKEN"):
        token = os.environ.get(name, "").strip()
        if re.fullmatch(r"[A-Za-z0-9_-]{1,4096}", token):
            return token

    gh = shutil.which("gh")
    if gh is None:
        return None
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.upper().startswith("ELIOT_")
    }
    env["GH_PROMPT_DISABLED"] = "1"
    try:
        result = subprocess.run(
            [gh, "auth", "token", "--hostname", "github.com"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            env=env,
            timeout=5,
            check=False,
        )
        if result.returncode == 0:
            token = result.stdout.decode("ascii").strip()
            if re.fullmatch(r"[A-Za-z0-9_-]{1,4096}", token):
                return token
    except (OSError, UnicodeError, subprocess.TimeoutExpired):
        pass
    return None


def github_request(url, headers):
    request = Request(url, headers=headers)
    if url.startswith("https://api.github.com/"):
        token = github_token()
        if token is not None:
            request.add_unredirected_header("Authorization", f"Bearer {token}")
    return request
