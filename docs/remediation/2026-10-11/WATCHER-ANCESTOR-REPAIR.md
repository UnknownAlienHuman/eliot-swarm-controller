# OpenCode watcher ancestor repair

The scoped override `@opencode/core@2.0.7 → @parcel/watcher@2.5.6` removes
`micromatch` and `braces`, including all dependency edges, from the 442-entry
lockfile. Direct OpenCode core/server/util remain 2.0.7; MCP client 2.2.0 and
its nested core records are unchanged. All 13 optional native watcher siblings
are pinned to 2.5.6. This uses the maintained upstream wrapper and binaries.

The real core resolves its native binding independently of the generic watcher
entrypoint. `Test-OpenCodeWatcherCompatibility.mjs` therefore imports the actual
core `filesystem/watcher-binding.js` loader and resolves `@parcel/watcher/wrapper`
from that core's dependency context. It does not substitute a binding or start
an OpenCode service or model.

## Verification

Fresh isolated locked installs pass on Windows x64 and Ubuntu x64/glibc with
Node 25.6.1, npm 11.9.0 and Bun 1.4.0. The wrapper and actual native sibling
resolve to 2.5.6 on both systems. Five cases pass on each: real core loader,
recursive creates, literal/dot-directory/glob/brace-pattern ignores, visible
updates, and absence of callbacks after unsubscribe. The Windows 2.5.1 baseline
also passes these cases. The existing MCP compatibility fixture passes 48/48
on Bun on each platform, and 48/48 on Windows Node.

Lockfile SHA-256:
`e280ba5f9206d11783bb4addb0ac6b98c2cca192ac641142cfed72de41a41435`.
Watcher fixture SHA-256:
`e9c71af559485297ea20b7c95c69a5f3ecd7da076918aaccea505c73cbccfa1e`.
Windows native watcher log SHA-256:
`4d0771bd51b5f9c09598ac003b636aa9c3e95b22f8b731a2628c044b26b50fd4`.
Linux watcher stdout SHA-256:
`c97d77100369fb761fa4e7b3d89103f629fd766aaa766c810cfb5a0d4969ca6b`.
The Linux runtime/download/command/closure handoff has SHA-256
`8e3ff0c6b5c394c29c24a33a19f4dce380e37bbf5e1acf94ed6524a0d4e4cc57`.

No Rust source changed, so the passed Rust gates are not repeated. These checks
establish bounded watcher and MCP compatibility; model consumption and full
installed qualification retain their separate gates. Historical installs,
frozen T binaries and qualification receipts remain unchanged. GitHub's
post-delivery alert state must be read back separately. The sprintf-js and
http-cache-semantics advisories remain unresolved.
