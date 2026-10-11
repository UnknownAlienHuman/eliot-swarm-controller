# R21. Delayed first RepairDispatch reuses retained same-content slot

**Status: focused production fix implemented; strict production Clippy and the fixed focused fixture passed on both Windows and Linux with stable source.** The published #47 v13 baseline at `eb85319` is not being requalified as a whole.

## Reproduced failure

The real O7 Store fixture `delayed_first_repair_reuses_middle_same_content_slot_after_feedback_without_repair` drives three assigned reviews through the existing review-disposition and repair consumer:

1. Review A reaches a reconciling owner binding. Its applied feedback Operation and immutable link are retained, while RepairDispatch remains pending and no `agent.send` is admitted.
2. The binding becomes ready. Review B has the same correction content, and the consumer queues its first `agent.send` RepairDispatch Operation under B's package-derived semantic slot.
3. Review C again has the same correction content. Feedback still points to A. The previous candidate lookup considered only C and A, so it missed B's retained slot and admitted a second `agent.send`.

The corrected Windows reproduction gate, edition `delayed-first-reproduce-v14-registration-fixed`, reached C and failed on the duplicate-send assertion with `source_stable=true`. The initial fixture ordering that failed before C was corrected by registering C's sponsored reviewer before A; that setup failure was not treated as reproduction evidence.

The three provenance digests differ while the correction-content digest is common:

| Review | `findings_digest` |
| --- | --- |
| A | `7a08c47ef91948b50c3ccdd9b31a9df6b484353523c1188802276fe0ad511002` |
| B | `42efd22f4c86b64aa7618ca5cc3d69b384ee5728a6bf6a0b665aeb138063fafa` |
| C | `d6b017b6a8b3fc0d689bb743e2976bad238cc2e2923ac06f5d05ab7a956fff72` |

All three packages produce correction-content digest `8416150a11bdea79d75aef1c6c2ae9074ad61b4b325fd826fde55a1646c47da1`.

## Digest and slot contract

`ReviewFindingsPackage::compute_digest` hashes canonical JSON containing `schema_version`, the full review-slot identity, `review_assignment_id`, `review_result_operation_id`, and the ordered findings. New RepairDispatch semantic slots use this provenance-specific `findings_digest`, so B and C intentionally have different slot IDs.

`correction_content_digest` hashes canonical JSON containing only the full review-slot identity and the ordered validated finding records. Canonical serialization normalizes JSON object-key order; the findings array order and finding values remain exact. The content digest omits `schema_version`, assignment ID, result Operation ID, and the package provenance digest. It identifies equivalent correction content, while it does not itself prove or select a retained Operation.

## Producer-to-consumer path

`consume_review_result_for_entry` reconstructs the current ordered package from the committed disposition and actionable findings. `RepairDispatchContext::from_committed_disposition` validates the retained feedback Operation, its package and immutable request, the owner binding, and equality of the current and feedback correction-content digests. For C, this correctly leaves A as the feedback provenance source.

The repair consumer now also reads the existing `entry_operation_links` pages for the exact effective manager, project, and automation. That reader validates each retained RepairDispatch link against its immutable `agent.send` Operation and semantic-slot receipt. The consumer keeps only packages with the exact full review-slot identity, current binding ID and generation, and matching correction-content digest. It adds those retained packages to the existing candidate set, then uses the existing semantic-slot resolver and ambiguity checks to read back B's exact unresolved slot and Operation. The queued Operation remains pending for exact delivery readback; no new Operation is admitted for C.

If no exact retained slot matches, new admission still follows the existing current `RepairDispatchContext` and authority checks. The link index and content digest are lookup evidence only; they do not supply authority or bypass the existing resolver.

## Verification status

The reproducer is the actual connected O7 Store test named above, not a helper-only fixture. Its corrected reproduction was executed on Windows before the production fix and failed at the expected duplicate-send assertion. Under edition `delayed-first-fixed-v15`, strict production Clippy passed on Windows and Linux with exit code 0 and `source_stable=true`; the fixed focused test also passed on both platforms. The Windows receipts are `review47-windows-clippy-delayed-first-fixed-v15.json` and `review47-windows-tests-delayed-first-fixed-v15.json`. The Linux receipts are `review47-linux-clippy-delayed-first-fixed-v15.json` and `review47-linux-tests-delayed-first-fixed-v15.json`; the Linux test ran one test and skipped 520. The Clippy command was `cargo +1.98.1 clippy --locked -p swarm-kernel-host --lib --bins --no-deps -- -D warnings` against verification base `3e6009fee4d92655701e789856370ae64a352b0d`. The checked repair consumer hash was `faa59db0ae2eeaa69a80522dcaa90a999184a0bb70f6712b61ba746cdb229980`, and the fixture hash was `7a6769993edeffdb0ccd2f6bfe9bcb0bbe5639076e967295eb975d8b44831810`. These focused gates do not qualify the full published #47 baseline.
