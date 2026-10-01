#!/usr/bin/env python3
"""Generate `src/vendor_rules.rs` from the betterleaks default rule config.

The betterleaks corpus (MIT, https://github.com/betterleaks/betterleaks) is the
industry rule table for known provider secret formats — ~325 rules covering the
prefixes and shapes that no entropy heuristic can be tuned to catch reliably.
Atlas vendors it as generated Rust data rather than taking a dependency, because
`atlas-redact` must stay pure (no I/O, no async, no network) and must compile
into the desktop binary with the rules baked in.

Usage:

    python3 scripts/gen_vendor_rules.py \\
        ~/go/pkg/mod/github.com/betterleaks/betterleaks@v1.5.0/config/betterleaks.toml

Re-run when bumping the vendored rule version, then run `cargo test` — the
`vendor_rules_all_compile` test is what proves the emitted regexes are valid for
Rust's `regex` crate (Go's RE2 syntax is close but not identical).

Deliberate exclusions, each for a reason:

  * `skipReport` rules — betterleaks detects these for *correlation* (an Alibaba
    access-key *id*, a Cloudinary cloud *name*, a Supabase project *url*) and
    does not report them as leaks. They are identifiers, not secrets; including
    them would redact ordinary configuration. Note the flag is not a reliable
    "not a secret" signal on its own: several rules carrying it match real
    credentials (an AWS *secret* access key, an OVH consumer key), so those are
    listed in KEEP_SKIP_REPORT and vendored anyway. Each is keyed — the pattern
    requires a credential-shaped key beside the value — which bounds the
    false-positive cost of keeping them.
  * `generic-api-key` — the corpus's own catch-all. It is the single largest
    source of false positives, and Atlas already covers that ground with the
    Shannon-entropy layer at a tuned threshold.
  * Rules with no content `regex` (path-only rules such as `pkcs12-file`) —
    `atlas-redact` sees strings, never file paths.
  * Any rule listed in INCOMPATIBLE, whose Go regex does not compile under
    Rust's `regex` crate. Kept as an explicit, reviewable list so a dropped rule
    is a recorded decision rather than a silent gap.
"""

from __future__ import annotations

import re
import sys
import tomllib
from pathlib import Path

# Rules whose Go regex has no Rust `regex` equivalent. Populated by running the
# `vendor_rules_all_compile` test and recording what it rejects; each entry
# should say why. An empty list means the whole corpus ported cleanly.
INCOMPATIBLE: dict[str, str] = {}

# Bounded repetitions that blow Rust's compiled-regex size limit, rewritten to an
# open upper bound. Rust builds a DFA up front where Go's RE2 does not, so a span
# like `[\w-]{50,1000}` costs megabytes to compile and both of these exceed the
# 10 MiB ceiling outright.
#
# The rewrite is safe *for redaction specifically*. Upstream, the upper bound
# bounds how much of a match gets reported; here the match becomes a replacement,
# and a greedy tail over the same character class can only ever cover more of the
# secret — never less, and never anything outside the class. It is applied per
# rule rather than to every wide range in the corpus, because for a rule whose
# wide range is a context window (`.{0,200}` spanning lines) the same rewrite
# would be badly wrong.
WIDE_RANGE_REWRITES: dict[str, tuple[str, str]] = {
    "pypi-upload-token": (r"[\w-]{50,1000}", r"[\w-]{50,}"),
    "vault-batch-token": (r"[\w-]{138,300}", r"[\w-]{138,}"),
}

# Rules excluded on false-positive grounds — see the module docstring.
EXCLUDED_IDS: dict[str, str] = {
    "generic-api-key": "catch-all; superseded by the entropy layer",
}

# `skipReport` rules that are nonetheless real credentials, vendored despite the
# flag. Every one is keyed (the regex requires a credential-shaped key beside the
# value), so keeping them does not open a false-positive hole.
KEEP_SKIP_REPORT: frozenset[str] = frozenset(
    {
        "aws-secret-access-key",
        "alibaba-sts-security-token",
        "cloudinary-api-key",
        "exoscale-api-secret",
        "ovh-application-key",
        "ovh-consumer-key",
        "polymarket-api-secret",
        "polymarket-passphrase",
    }
)

# `filter = 'entropy(finding["secret"]) <= 3.8'` (and the `filter.entropy(...)`
# spelling the generator emits for some rules). The comparison says when to
# DISCARD a finding, so a rule fires only above the threshold.
ENTROPY_FILTER = re.compile(
    r'(?:filter\.)?entropy\(finding\["secret"\]\)\s*(<=|<)\s*([0-9.]+)'
)


def rust_str(value: str) -> str:
    """Emit a Rust raw string literal with enough `#` to survive the content."""
    hashes = 1
    while ('"' + "#" * hashes) in value:
        hashes += 1
    pad = "#" * hashes
    return f'r{pad}"{value}"{pad}'


def parse_entropy(rule: dict) -> tuple[float, bool] | None:
    """Return (threshold, discard_is_inclusive) for a rule's entropy filter."""
    match = ENTROPY_FILTER.search(rule.get("filter", ""))
    if not match:
        return None
    return float(match.group(2)), match.group(1) == "<="


def main() -> int:
    if len(sys.argv) != 2:
        print(__doc__)
        return 2

    config = tomllib.loads(Path(sys.argv[1]).read_text())
    out: list[str] = []
    skipped: dict[str, str] = {}

    for rule in config["rules"]:
        rule_id = rule["id"]
        if rule_id in EXCLUDED_IDS:
            skipped[rule_id] = EXCLUDED_IDS[rule_id]
            continue
        if rule_id in INCOMPATIBLE:
            skipped[rule_id] = INCOMPATIBLE[rule_id]
            continue
        if rule.get("skipReport") and rule_id not in KEEP_SKIP_REPORT:
            skipped[rule_id] = "skipReport: identifier, not a secret"
            continue
        pattern = rule.get("regex")
        if not pattern:
            skipped[rule_id] = "no content regex"
            continue
        if rule_id in WIDE_RANGE_REWRITES:
            old, new = WIDE_RANGE_REWRITES[rule_id]
            if old not in pattern:
                raise SystemExit(
                    f"{rule_id}: WIDE_RANGE_REWRITES expects {old!r}, which is no longer "
                    f"in the upstream pattern — re-check the rewrite before regenerating"
                )
            pattern = pattern.replace(old, new)

        keywords = [k.lower() for k in rule.get("keywords", [])]
        entropy = parse_entropy(rule)
        out.append(
            "    VendorRule {\n"
            f"        id: {rust_str(rule_id)},\n"
            f"        pattern: {rust_str(pattern)},\n"
            f"        keywords: &[{', '.join(rust_str(k) for k in keywords)}],\n"
            f"        secret_group: {rule.get('secretGroup', 0)},\n"
            + (
                f"        min_entropy: Some({entropy[0]!r}),\n"
                if entropy
                else "        min_entropy: None,\n"
            )
            + f"        entropy_inclusive: {str(bool(entropy and entropy[1])).lower()},\n"
            "    },"
        )

    header = f"""//! Vendored betterleaks secret-detection rules — GENERATED, do not edit.
//!
//! Source: betterleaks {config.get("betterleaksMinVersion", "v1.5.0")} default config (MIT).
//! Regenerate with `scripts/gen_vendor_rules.py <path-to-betterleaks.toml>`.
//!
//! {len(out)} rules vendored, {len(skipped)} skipped. Skips are listed in the
//! generator, not here, so the reason survives regeneration.

use super::vendor::VendorRule;

/// Every vendored rule, in corpus order.
pub static VENDOR_RULES: &[VendorRule] = &[
{chr(10).join(out)}
];
"""

    dest = Path(__file__).resolve().parent.parent / "src" / "vendor_rules.rs"
    dest.write_text(header)
    print(f"wrote {dest} — {len(out)} rules, {len(skipped)} skipped")
    for rule_id, reason in sorted(skipped.items()):
        print(f"  skip {rule_id}: {reason}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
