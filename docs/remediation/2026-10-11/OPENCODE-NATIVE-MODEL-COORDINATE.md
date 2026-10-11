# OpenCode native model coordinate

The native qualification parameter now accepts and preserves the full Kilo
coordinate `kilo/stepfun/step-5-preview-free`. It separates only the first
provider boundary, so the native tuple is `providerID=kilo`,
`id=stepfun/step-5-preview-free`; the configured `high` variant is unchanged.
The previous flattened id remains opaque when explicitly supplied; it is
not silently rewritten or treated as proof of native catalog membership.
Empty components, spaces, backslashes and control characters are refused.

The standalone fixture reads the actual parameter validation/default and
executes the actual `Get-AdapterContract`. It passes **21/21 on Windows and
Linux**, with zero native/model calls. This PowerShell-only change needs no
Rust rebuild or repeated Rust gates. It preserves the previously reviewed
terminal-rejection and readiness helpers.

| File or check | SHA-256 |
| --- | --- |
| Native harness, Windows working-copy bytes | `81540b75284e05ed09ee94899df2ea5fe9d66eb82eabcfc7e5933d0e2c06d921` |
| Coordinate fixture | `70c63be2a9aa58069056e714fbdb6195637caba7861c9ef5d2064941c6f40c5d` |
| Both coordinate logs | `e1fbb1afe15172e3f9faeb9489ade9094ee65f6a3acadbd4022d5115d781118a` |

The installed core2.0.7 source points to
`https://models.opencode.ai/api.json`. Its unauthenticated readback at
`2026-10-11T08:56:33Z` lists this exact Kilo Step 5 free id and high effort;
the shipped snapshot lacks Step 5 free. This repairs expression of the requested
model. The failed T server's actual catalog and refresh outcome were not
recovered. A future run still requires exact selected-server catalog evidence
before model dispatch. No historical run is replayed and public catalog
visibility alone is not qualification.
