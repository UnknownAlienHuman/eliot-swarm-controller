# Contract-decision V2 continuation checkpoint

Status: `INCOMPLETE_PRIVATE_SNAPSHOT`; do not apply or label READY.

V1 remains preserved and immutable at `.local/pr-implementation/pr25-contract-decisions-source-v1/`. Its manifest SHA is `A53F3CEF1ED4C8E858D88E993AF0F846617F3918DA0BC925C96E782658D0D556`, source-proof SHA is `4BFBF450703604143F6AE3D764CA5B12D821B8E66911B6435012BBF3C864E3E6`, and candidate tree SHA is `e5afa8205a466f19830ceebc3ee53fe3a3a20c08ae06da33c29e909c6cf0799c`. Root has explicitly decided not to expose the unfinished decision methods.

The current V2 candidate files are private working copies, with no V2 manifest, proof, or READY marker:

- `candidate/crates/swarm-kernel-host/src/coordination/contract_decision.rs`: SHA-256 `461A1DB0A47836476374487E73AC6FF276D95AACE98D0535B4C400B11B371490`, 7,901 bytes.
- `candidate/crates/swarm-kernel-host/src/store/contract_decisions.rs`: SHA-256 `0CF4155CF5791F9FFD939FFFA4F7783A03A73570A611EBB111D9C49D104FC9C8`, 17,626 bytes.
- Current two-file candidate tree SHA-256: `746cb0fc2a4cce38036e1458dd90221f5139bc0014a2da1bd1879ca959405aae` using the V1 sorted-path/hash recipe.

V2 was partially reduced to use the existing proposal list cursor and exact `read_for_revision` helper; the unused independent decision list/index was removed. The root-identified defect remains unfixed: `has_unsupported_glob` treats the supported terminal `/**` as unsupported, so determinately disjoint prefix scopes may be reported as unknown. Before any future READY state, fix that classifier while preserving unknown for genuinely unsupported globs, then regenerate an exact V2 manifest/proof/READY. Root's required integration hunks are: declare the DTO and Store modules; add strict model validation and method-policy entries for the two canonical mutations; route Store mutation dispatch to `contract_decisions::apply`; preserve pre-savepoint exact Thread Task/Attempt stamping; integrate `read_for_revision` into `coordination.contract.get` for its requested revision and into existing bounded `coordination.contract.list` for each returned proposal head without changing its cursor.

No tracked files were written by this packet. No compiler, Cargo, tests, database, native, process, or Git mutation was used. V2 has not been verified or frozen.

Git-retained copies: contract_decision.rs.txt and contract_decisions.rs.txt preserve the exact candidate bytes/hashes above. They are unfinished source, outside compiled production modules. Restore them only when the owner resumes; finish the documented classifier and shared/public glue before exposing ratify/reject.
