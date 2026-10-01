# JavaScript provenance in the Rust controller

Checked 2026-10-01. The JavaScript files belong to the module-local **Muse SDK bridge**, not the Rust host, database, task authority or OpenCode adapter. There is no browser UI or Node server in the controller core.

The dependency was introduced in [0e3ccd6b](https://github.com/UnknownAlienHuman/eliot-swarm-controller/commit/0e3ccd6b7db5ee0665280be70e635642515cfccf), **2026-09-30 09:29:09 UTC / 05:29:09 New York**, to reuse the complete official SDK instead of writing another MSP transport. That checkpoint did not yet implement the bridge.

The first runnable bridge was added in [b2bd0211](https://github.com/UnknownAlienHuman/eliot-swarm-controller/commit/b2bd0211e772f273494e808485a1af029588f74b), **2026-09-30 11:30:01 UTC / 07:30:01 New York**. Subsequent commits extended native controls, results, checkpointing and recovery.

The current six source files are `modules/muse/{bridge,checkpoint,control,owned,results,settings}.mjs`. The module's private `package.json` requires Node >=22 and pins `@muse-code/sdk` **1.3.0**, with a retained `package-lock.json`; it is not an unpinned global npm dependency. The bridge imports the official SDK and translates binding-scoped commands, native observations and receipts. SQLite transactions, task ownership/acceptance, durable effect admission, host IPC and OS process ownership remain in Rust.

This is permitted by the [module contract §11](agent_swarm.module-contract-v2.md): whole official SDK reuse, module-local Node/Python and separate pinned adaptation rather than a duplicate transport implementation. A pure-Rust rewrite of Muse would require a separate decision about replacing the official SDK; deleting these files now would remove the working Muse integration, not merely clean generated files.

The new direct OpenCode V2 adapter is entirely Rust. Atlas's unchanged donor snapshot additionally retains its upstream Python rule-generation script for provenance; that script is not part of the controller build or runtime. Language percentages include retained source and do not describe runtime architecture. `.gitattributes` contains ordinary file-format rules, with no Linguist suppression added to disguise the composition.
