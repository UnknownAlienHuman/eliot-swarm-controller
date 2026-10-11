# OpenCode catalog and R48 worker source repair

Status: affected source checks pass; installed/native qualification remains pending.

OpenCode now carries one closed catalog-failure category alongside its existing
`NATIVE_MODEL_UNAVAILABLE` diagnostic. The same bounded catalog wait and exact
provider/model/enabled/variant predicates remain in force before native root
creation. HTTP, schema and directory failures retain their original codes. The
Store's public `operation.get` projects only `{code, catalog_failure_category}`
for a rejected `agent.open` with a recognized category. Arbitrary private
details never become a fallback. Existing historical OC3 observations remain
unchanged; this repair does not reconstruct their missing category.

The Store-owned R48 worker launch now appends its optional qualification flag
after `run --config <path>`. Release-default argv and feature gating remain
unchanged. The actual unchanged installed feature worker accepts the corrected
order and reaches `AUTOMATION_CONFIG_MISSING` for a deliberately absent config;
the historical order yields `INVALID_PARAMS`. Both bounded parser checks stop
before a service group, Store connection or scheduler work. These checks do not
qualify the full installed host/worker contour.

## Verification

- Rust 1.98.1 scoped formatting and `git diff --check` pass.
- Strict production Clippy (`--lib --bins --no-deps -- -D warnings`) passes on
  Windows and Linux for OpenCode/host default code and the host qualification
  feature. The first run found an unused compatibility wrapper; it was removed
  rather than suppressing the warning. That failed run remains retained.
- The three catalog regressions pass on each platform: read-only refresh until
  the exact model appears, immediate malformed-catalog rejection and all four
  failure classifications.
- The public Store `operation.get` regression passes on each platform for all
  four categories, private-data exclusion, malformed category, wrong code,
  another method and an applied outcome. Its first run failed while inserting
  fixture rows without the schema-required terminal timestamp. The fixture now
  supplies the timestamp; only this failed selection was repeated.
- The actual feature-worker parser checks above pass 2/2 on Windows. The first
  wrapper assertion omitted the worker's legitimate bounded fault-armed line;
  the corrected closed output assertion passes and no worker defect is inferred
  from that wrapper failure.

After production Clippy, only two `cfg(test)` adjustments changed: the malformed
catalog fixture reads the wrapped error, and the Store fixture supplies its
terminal timestamp. Reversing those exact test edits reproduces the recorded
Clippy hashes. Production bodies remain identical to the checked code; the
catalog and public Store tests verify the final test source.

The earlier Antigravity 14-case selection and full 24-package Clippy are not repeated.
Fresh matching installed artifacts are required for the repaired Antigravity
producer, OpenCode diagnostic and R48 host argv. Model consumption, native
result-body causality and independent Task acceptance remain unverified.
