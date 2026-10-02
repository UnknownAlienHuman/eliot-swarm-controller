//! Uniform post-projection limits and the typed projection frame
//! (Documentation Program §8.1, R6/R22 remainder).
//!
//! Artifact reads are already bounded before accumulation
//! (`artifacts::MAX_PAGE_BYTES` range reads) and result scans cap bytes
//! before parsing. Report projections are different: they are built from
//! retained rows first and only then returned, so their limits apply
//! AFTER projection, on the projected items themselves:
//!
//! - `max_source_rows` — the caller's row cap on the source fetch
//!   (unchanged semantics; enforced by the query, recorded in the frame);
//! - [`MAX_PROJECTED_ITEMS`] — projected items per page;
//! - [`MAX_SERIALIZED_BYTES`] — canonical serialized bytes of the
//!   projected items array of one page;
//! - [`MAX_SINGLE_ITEM_BYTES`] — canonical serialized bytes of one
//!   projected item.
//!
//! An item that exceeds the single-item cap is irreducible for a bounded
//! preview: it is replaced in place by an explicit gap reference carrying
//! its identity, exact byte length and payload digest. Nothing is ever
//! truncated silently and a page never claims continuity across a gap:
//! the frame reports `coverage_complete: false` with a typed
//! `gap_reason`. The full bytes stay where they always were — artifact
//! payloads remain readable through the existing artifact range reads,
//! observation payloads remain in the retained observation record the
//! reference names.
//!
//! Cursor honesty: a page's `next_cursor` advances only over entries the
//! consumer actually received (a full item or an explicit gap reference).
//! When a byte/item budget stops a page early, the unemitted source rows
//! are not consumed; the next page, polled from `next_cursor`, returns
//! them — no replay of emitted entries, no silent skip.

use crate::{error::Result, model};
use serde_json::{Value, json};

/// Projected items per page. Matches the existing `page()` ceiling
/// (limit 1..=200), so the post-projection item cap never surprises a
/// consumer the source-row cap already admitted.
pub(super) const MAX_PROJECTED_ITEMS: usize = 200;
/// Canonical serialized bytes of one page's projected items array:
/// four artifact pages. A delta page may carry several bounded payloads
/// and stay a bounded preview, far below the 8 MiB result-scan cap.
pub(super) const MAX_SERIALIZED_BYTES: usize = 262_144;
/// Canonical serialized bytes of one projected item: exactly one
/// artifact page (`artifacts::MAX_PAGE_BYTES`). Anything larger is
/// irreducible for a bounded preview and becomes a gap reference.
pub(super) const MAX_SINGLE_ITEM_BYTES: usize = crate::artifacts::MAX_PAGE_BYTES;

pub(super) const GAP_ITEM_EXCEEDS_SINGLE: &str = "item_exceeds_single_item_bytes";
pub(super) const GAP_ITEM_EXCEEDS_PAGE: &str = "item_exceeds_page_byte_budget";

/// The outcome of applying the post-projection limits to one page.
pub(super) struct Limited {
    /// Projected items as returned: full items and gap references,
    /// in source order.
    pub items: Vec<Value>,
    /// Source entries consumed by this page (emitted in full or as a
    /// gap reference). Source entries after `consumed` were not touched.
    pub consumed: usize,
    /// Canonical serialized byte length of `items` as an array.
    pub serialized_byte_length: usize,
    /// First gap reason on this page, if any item was detached.
    pub gap_reason: Option<&'static str>,
    /// How many items were detached as gap references on this page.
    pub gap_count: usize,
    /// A budget stopped the page before every source entry was emitted.
    pub stopped_early: bool,
}

/// Applies the post-projection limits to `entries` (already projected,
/// in source order). `gap_reference` builds the explicit detached
/// reference for one oversized entry from the entry itself, the typed
/// reason and the entry's canonical serialized byte length.
pub(super) fn limit_items(
    entries: Vec<Value>,
    mut gap_reference: impl FnMut(&Value, &'static str, usize) -> Result<Value>,
) -> Result<Limited> {
    let mut items: Vec<Value> = Vec::new();
    let mut bytes = 2usize; // canonical "[]"
    let mut gap_reason: Option<&'static str> = None;
    let mut gap_count = 0usize;
    let mut consumed = 0usize;
    let mut stopped_early = false;
    for entry in &entries {
        let serialized = model::canonical(entry)?;
        let len = serialized.len();
        let (candidate, reason) = if len > MAX_SINGLE_ITEM_BYTES {
            (
                gap_reference(entry, GAP_ITEM_EXCEEDS_SINGLE, len)?,
                Some(GAP_ITEM_EXCEEDS_SINGLE),
            )
        } else {
            (entry.clone(), None)
        };
        let candidate_len = model::canonical(&candidate)?.len();
        let added = candidate_len + usize::from(!items.is_empty());
        if items.len() >= MAX_PROJECTED_ITEMS || bytes + added > MAX_SERIALIZED_BYTES {
            if items.is_empty() && reason.is_none() {
                // The entry fits the single-item cap but not an empty
                // page budget (reachable only if the two constants are
                // ever tuned across each other). Detach it as a gap
                // reference rather than returning an empty page whose
                // cursor could never advance.
                let gap = gap_reference(entry, GAP_ITEM_EXCEEDS_PAGE, len)?;
                let gap_len = model::canonical(&gap)?.len();
                items.push(gap);
                bytes += gap_len;
                consumed += 1;
                gap_count += 1;
                gap_reason = Some(GAP_ITEM_EXCEEDS_PAGE);
                stopped_early = entries.len() > consumed;
                break;
            }
            stopped_early = true;
            break;
        }
        items.push(candidate);
        bytes += added;
        consumed += 1;
        if let Some(reason) = reason {
            gap_count += 1;
            if gap_reason.is_none() {
                gap_reason = Some(reason);
            }
        }
    }
    Ok(Limited {
        items,
        consumed,
        serialized_byte_length: bytes,
        gap_reason,
        gap_count,
        stopped_early,
    })
}

/// The typed projection frame of §8.1, shared by the store's report
/// projections. `range` carries the projection's own cursor/range
/// identity; `coverage_complete` is decided by the caller (a timeline
/// page is complete iff nothing was detached; a family page also
/// requires the retained enumeration itself to be complete).
#[allow(clippy::too_many_arguments)]
pub(super) fn frame(
    source_kind: &str,
    range: Value,
    limited: &Limited,
    max_source_rows: i64,
    has_older: bool,
    has_newer: bool,
    coverage_complete: bool,
    retained_stale_members: Vec<Value>,
) -> Result<Value> {
    Ok(json!({
        "source_kind": source_kind,
        "projection_revision": model::digest(
            model::canonical(&Value::Array(limited.items.clone()))?.as_bytes(),
        ),
        "range": range,
        "coverage_complete": coverage_complete,
        "has_older": has_older,
        "has_newer": has_newer,
        "gap_reason": limited.gap_reason,
        "gap_count": limited.gap_count,
        "retained_stale_members": retained_stale_members,
        "serialized_byte_length": limited.serialized_byte_length,
        "limits": {
            "max_source_rows": max_source_rows,
            "max_projected_items": MAX_PROJECTED_ITEMS,
            "max_serialized_bytes": MAX_SERIALIZED_BYTES,
            "max_single_item_bytes": MAX_SINGLE_ITEM_BYTES,
        },
    }))
}

/// Gap reference for one timeline (report.delta / message.read) entry.
/// Keeps the entry's cursor, kind and timestamp so the consumer's cursor
/// advances over an explicitly received reference, never over silently
/// dropped bytes. When the payload itself references an artifact, the
/// reference names it: the full bytes remain available through the
/// existing artifact range reads.
pub(super) fn timeline_gap_reference(
    item: &Value,
    reason: &'static str,
    item_serialized_bytes: usize,
) -> Result<Value> {
    let payload = &item["payload"];
    let payload_canonical = model::canonical(payload)?;
    let mut gap = json!({
        "reason": reason,
        "item_serialized_bytes": item_serialized_bytes,
        "max_single_item_bytes": MAX_SINGLE_ITEM_BYTES,
        "payload_serialized_bytes": payload_canonical.len(),
        "payload_digest": model::digest(payload_canonical.as_bytes()),
        "detached_reference": {
            "kind": "observation",
            "observation_id": item["cursor"],
        },
    });
    let artifact_ref = payload["details"]["artifact_ref"]
        .as_str()
        .or_else(|| payload["artifact_ref"].as_str());
    if let Some(artifact_ref) = artifact_ref {
        gap["artifact_ref"] = json!(artifact_ref);
        gap["detached_reference"] = json!({
            "kind": "artifact",
            "artifact_id": artifact_ref,
            "range_reads": "artifact.read",
            "observation_id": item["cursor"],
        });
    }
    Ok(json!({
        "cursor": item["cursor"],
        "kind": item["kind"],
        "recorded_at_ms": item["recorded_at_ms"],
        "gap": gap,
    }))
}

/// Gap reference for one family-projection member entry. The member's
/// session identity is kept; its full entry remains in the retained
/// observation the frame's range names.
pub(super) fn family_gap_reference(
    item: &Value,
    reason: &'static str,
    item_serialized_bytes: usize,
) -> Result<Value> {
    let canonical = model::canonical(item)?;
    Ok(json!({
        "sessionId": item["sessionId"],
        "gap": {
            "reason": reason,
            "item_serialized_bytes": item_serialized_bytes,
            "max_single_item_bytes": MAX_SINGLE_ITEM_BYTES,
            "item_digest": model::digest(canonical.as_bytes()),
            "detached_reference": {
                "kind": "family_member",
                "session_id": item["sessionId"],
            },
        },
    }))
}

#[cfg(test)]
mod tests {
    use super::super::{producers, read};
    use super::*;
    use crate::{config::Config, model::Role};
    use rusqlite::{Connection, params};

    fn principal(client_id: &str) -> model::Principal {
        model::Principal {
            link_id: "link-1".into(),
            client_id: client_id.into(),
            role: Role::Operator,
        }
    }

    fn fixture_db() -> Connection {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(super::super::SCHEMA).unwrap();
        db
    }

    fn insert_observation(db: &Connection, id: i64, kind: &str, payload: &Value) {
        db.execute(
            "INSERT INTO observations(observation_id,source_stream_id,source_event_key,kind,payload_json,recorded_at_ms) VALUES(?1,'test',?2,?3,?4,?5)",
            params![
                id,
                format!("k{id}"),
                kind,
                model::canonical(payload).unwrap(),
                1_000 + id
            ],
        )
        .unwrap();
    }

    fn delta(db: &Connection, after: i64, limit: i64) -> Value {
        read(
            db,
            &principal("op-1"),
            "report.delta",
            &json!({"after": after, "limit": limit}),
            &Config::default(),
        )
        .unwrap()
    }

    #[test]
    fn oversized_item_becomes_gap_reference_and_cursor_stays_honest() {
        let db = fixture_db();
        insert_observation(&db, 1, "runtime.state", &json!({"note": "small"}));
        let big_text = "x".repeat(MAX_SINGLE_ITEM_BYTES);
        let big_payload = json!({"details": {"artifact_ref": "art-9"}, "blob": big_text});
        insert_observation(&db, 2, "runtime.result", &big_payload);
        insert_observation(&db, 3, "runtime.state", &json!({"note": "after"}));

        let page = delta(&db, 0, 50);
        let items = page["items"].as_array().unwrap();
        assert_eq!(items.len(), 3, "{page}");
        assert_eq!(items[0]["payload"]["note"], "small");
        // The oversized item is present as an explicit gap reference at
        // its own cursor — not truncated, not skipped.
        let gap_item = &items[1];
        assert_eq!(gap_item["cursor"], 2);
        assert!(gap_item.get("payload").is_none(), "{gap_item}");
        assert_eq!(gap_item["gap"]["reason"], GAP_ITEM_EXCEEDS_SINGLE);
        assert_eq!(gap_item["gap"]["artifact_ref"], "art-9");
        assert_eq!(
            gap_item["gap"]["detached_reference"]["kind"], "artifact",
            "{gap_item}"
        );
        assert_eq!(
            gap_item["gap"]["payload_digest"],
            model::digest(model::canonical(&big_payload).unwrap().as_bytes())
        );
        assert_eq!(items[2]["payload"]["note"], "after");
        // No false continuity across the gap.
        let frame = &page["projection"];
        assert_eq!(frame["coverage_complete"], false, "{frame}");
        assert_eq!(frame["gap_reason"], GAP_ITEM_EXCEEDS_SINGLE);
        assert_eq!(frame["gap_count"], 1);
        assert_eq!(frame["source_kind"], "observation_timeline");
        assert!(frame["serialized_byte_length"].as_u64().unwrap() <= MAX_SERIALIZED_BYTES as u64);
        // The cursor advanced over exactly what was received; the next
        // poll replays nothing and the stream reports its end honestly.
        assert_eq!(page["next_cursor"], 3);
        assert_eq!(frame["has_newer"], false);
        let tail = delta(&db, page["next_cursor"].as_i64().unwrap(), 50);
        assert_eq!(tail["items"].as_array().unwrap().len(), 0);
        assert_eq!(tail["projection"]["coverage_complete"], true);
        assert_eq!(tail["projection"]["has_newer"], false);
    }

    #[test]
    fn page_byte_budget_stops_early_and_resumes_without_replay() {
        let db = fixture_db();
        // Each item is below the single-item cap but six of them exceed
        // one page's serialized budget.
        let filler = "y".repeat(MAX_SINGLE_ITEM_BYTES - 4_096);
        for id in 1..=6 {
            insert_observation(&db, id, "runtime.state", &json!({"blob": filler}));
        }
        let first = delta(&db, 0, 50);
        let first_items = first["items"].as_array().unwrap();
        assert!(!first_items.is_empty());
        assert!(first_items.len() < 6, "{first}");
        assert!(
            first["projection"]["serialized_byte_length"]
                .as_u64()
                .unwrap()
                <= MAX_SERIALIZED_BYTES as u64
        );
        // A budget stop is not a gap: coverage of the returned range is
        // complete and the remainder is honestly reported as newer.
        assert_eq!(first["projection"]["coverage_complete"], true);
        assert_eq!(first["projection"]["gap_count"], 0);
        assert_eq!(first["projection"]["has_newer"], true);
        let next = first["next_cursor"].as_i64().unwrap();
        assert_eq!(next, first_items.len() as i64);
        let second = delta(&db, next, 50);
        let second_items = second["items"].as_array().unwrap();
        assert_eq!(second_items.len(), 6 - first_items.len());
        assert_eq!(second_items[0]["cursor"], next + 1, "no replay, no skip");
        assert_eq!(second["projection"]["has_newer"], false);
    }

    #[test]
    fn projected_item_cap_stops_the_page() {
        let entries: Vec<Value> = (0..(MAX_PROJECTED_ITEMS + 5))
            .map(|i| json!({"cursor": i, "kind": "k", "payload": {}, "recorded_at_ms": 0}))
            .collect();
        let limited = limit_items(entries, timeline_gap_reference).unwrap();
        assert_eq!(limited.items.len(), MAX_PROJECTED_ITEMS);
        assert_eq!(limited.consumed, MAX_PROJECTED_ITEMS);
        assert!(limited.stopped_early);
        assert_eq!(limited.gap_count, 0);
    }

    #[test]
    fn message_read_shares_the_frame_with_mailbox_source_kind() {
        let db = fixture_db();
        insert_observation(
            &db,
            1,
            "message.send",
            &json!({"recipient": "op-1", "text": "hello"}),
        );
        insert_observation(&db, 2, "runtime.state", &json!({"note": "not mail"}));
        let page = read(
            &db,
            &principal("op-1"),
            "message.read",
            &json!({"after": 0, "limit": 50}),
            &Config::default(),
        )
        .unwrap();
        let items = page["items"].as_array().unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0]["kind"], "message.send");
        assert_eq!(page["projection"]["source_kind"], "mailbox");
        assert_eq!(page["projection"]["coverage_complete"], true);
        assert_eq!(page["projection"]["limits"]["max_source_rows"], 50);
    }

    fn family_fixture(children: Value, completeness: &str) -> (Connection, Value) {
        let db = fixture_db();
        db.execute(
            "INSERT INTO bindings(binding_id,generation,lane_id,module_instance_id,module_artifact_id,state,route_json,state_json,created_at_ms) VALUES('b1',1,'lane-1','inst-1','art-1','ready','{}','{}',1000)",
            [],
        )
        .unwrap();
        let state = json!({
            "native_root_id": "ses_root",
            "session": {"sessionId": "ses_root"},
            "observed_children": children,
            "turns": [],
            "family_completeness": completeness,
            "gaps": 1,
        });
        db.execute(
            "INSERT INTO observations(observation_id,source_stream_id,source_event_key,binding_id,binding_generation,kind,payload_json,recorded_at_ms) VALUES(7,'test','fam','b1',1,'runtime.state',?1,5000)",
            params![model::canonical(&state).unwrap()],
        )
        .unwrap();
        let request = json!({"binding_id": "b1", "generation": 1, "observation_id": 7});
        (db, request)
    }

    #[test]
    fn family_frame_carries_retained_stale_members_not_terminal() {
        // ses_gone disappeared from the newest native enumeration; the
        // retained snapshot keeps it with observed_now=false (§13 #10):
        // the projection frame must carry it as retained stale, and the
        // member itself must stay in the page, never marked terminal.
        let children = json!([
            {"sessionId": "ses_now", "observed_now": true},
            {"sessionId": "ses_gone", "observed_now": false},
        ]);
        let (db, request) = family_fixture(children, "partial");
        let page = producers::family(&db, &request).unwrap();
        assert_eq!(page["family_completeness"], "partial");
        let frame = &page["projection"];
        assert_eq!(frame["source_kind"], "family_observation");
        assert_eq!(frame["coverage_complete"], false, "{frame}");
        assert_eq!(
            frame["retained_stale_members"],
            json!(["ses_gone"]),
            "{frame}"
        );
        let items = page["items"].as_array().unwrap();
        assert_eq!(items.len(), 2);
        let gone = items
            .iter()
            .find(|c| c["sessionId"] == "ses_gone")
            .expect("retained member stays in the page");
        assert_eq!(gone["observed_now"], false);
        assert!(gone.get("terminal").is_none(), "{gone}");
        assert!(gone.get("gap").is_none(), "{gone}");
    }

    #[test]
    fn family_oversized_member_becomes_gap_reference() {
        let big = "z".repeat(MAX_SINGLE_ITEM_BYTES);
        let children = json!([
            {"sessionId": "ses_big", "observed_now": true, "blob": big},
            {"sessionId": "ses_small", "observed_now": false},
        ]);
        let (db, request) = family_fixture(children, "partial");
        let page = producers::family(&db, &request).unwrap();
        let items = page["items"].as_array().unwrap();
        assert_eq!(items.len(), 2, "{page}");
        assert_eq!(items[0]["sessionId"], "ses_big");
        assert_eq!(items[0]["gap"]["reason"], GAP_ITEM_EXCEEDS_SINGLE);
        assert_eq!(items[1]["sessionId"], "ses_small");
        assert_eq!(page["projection"]["coverage_complete"], false);
        assert_eq!(page["projection"]["gap_count"], 1);
        // The whole retained inventory was still represented, so the
        // page range closed over it.
        assert_eq!(page["enumeration_complete"], true);
        assert_eq!(page["next_after"], Value::Null);
        assert_eq!(
            page["projection"]["retained_stale_members"],
            json!(["ses_small"])
        );
    }
}
