# Remaining dependency advisories

The [scoped watcher ancestor repair](WATCHER-ANCESTOR-REPAIR.md) now removes
braces and micromatch from the source lock and fresh Windows/Linux installs.
Native watcher and MCP compatibility pass on both systems. Post-delivery
GitHub readback on 2026-10-11 confirms alert #12 fixed at
`2026-10-11T05:22:32Z`; alerts #13 and #15 remain open. The two other advisory
repairs remain open. The three-alert table below is the retained pre-repair
readback, not the current alert count.

Live GitHub/advisory/registry readback at `2026-10-11T04:32:36Z` retains three
open alerts. Alert #16, the MCP client, is fixed at 2.2.0 and was confirmed fixed by
GitHub at `2026-10-11T02:44:56Z`.

| Alert | Package | Affected range | Current upstream evidence |
| --- | --- | --- | --- |
| [15](https://github.com/UnknownAlienHuman/eliot-swarm-controller/security/dependabot/15) | sprintf-js | `<=1.1.3` | Latest 1.1.3; no declared patched release |
| [13](https://github.com/UnknownAlienHuman/eliot-swarm-controller/security/dependabot/13) | http-cache-semantics | `<=4.2.0` | Latest 4.3.0, released on 2026-10-04; the named max-stale block is unchanged and a fix is unverified |
| [12](https://github.com/UnknownAlienHuman/eliot-swarm-controller/security/dependabot/12) | braces | `<=3.0.3` | Latest 3.0.3; no declared patched release |

The remaining locked chain includes core 2.0.7 → gray-matter 4.0.3 → js-yaml 3.15.2 →
argparse 1.0.10 → sprintf-js 1.0.3. The former watcher 2.5.1 → micromatch 4.0.8 →
braces 3.0.3 chain is removed by the scoped watcher 2.5.6 override. The cache package comes through the native npm
installation dependency chain ending in make-fetch-happen 15.0.6.

The range `<=3.0.3` includes braces 2.x as well; a downgrade is not a repair.
The leaf-version readback alone does not establish whether a compatible
ancestor upgrade can remove the vulnerable package. That separate source/API
assessment remains pending. No alert is dismissed and no package is relabelled
fixed. Native reachability and exploitability are not established by this
readback. The original current-state report is retained at SHA-256
`5bd097d87affc4e5579216ce1c3d17cf7e42160ac42f7aa1e180c7d6ffc8f036`;
its incorrect braces downgrade note is superseded by the range correction here.
