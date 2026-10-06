use super::subscriptions::*;
use serde_json::{Value, json};
fn item(kind: &str, operation_id: Option<&str>, payload: Value) -> Value {
    json!({
        "cursor": 7,
        "kind": kind,
        "payload": payload,
        "recorded_at_ms": 1_700_000_000_000_i64,
        "operation_id": operation_id,
    })
}

#[test]
fn categories_match_only_committed_stream_facts() {
    let all = [
        Category::Reports,
        Category::Mailbox,
        Category::Operations,
        Category::Concilium,
    ];
    // An Operation admission: a report entry and an operation
    // transition, not a mailbox delivery.
    let admission = item("task.create", Some("op-1"), json!({"task_id": "t-1"}));
    assert_eq!(
        matched_categories(&all, &admission, "operator"),
        [Category::Reports, Category::Operations]
    );
    // A mailbox delivery to the facade's own client: all applicable --
    // it is a committed report entry, a delivery, and the send
    // Operation's admission.
    let mail = item(
        "message.send",
        Some("op-2"),
        json!({"recipient": "operator", "delivery_id": "d-1"}),
    );
    assert_eq!(
        matched_categories(&all, &mail, "operator"),
        [Category::Reports, Category::Mailbox, Category::Operations]
    );
    // The same delivery addressed to another client is not this
    // facade's mailbox (message.read is recipient-scoped).
    assert_eq!(
        matched_categories(&all, &mail, "someone-else"),
        [Category::Reports, Category::Operations]
    );
    // A native observation with no Operation and no recipient:
    // reports only.
    let native = item("runtime.state", None, json!({"note": "observed"}));
    assert_eq!(
        matched_categories(&all, &native, "operator"),
        [Category::Reports]
    );
    // A gap reference (oversized payload detached by the store):
    // the payload -- and with it the recipient -- is not inline, so
    // mailbox cannot claim it; identity categories still match.
    let gap = json!({
        "cursor": 9,
        "kind": "message.send",
        "recorded_at_ms": 1_700_000_000_000_i64,
        "operation_id": "op-3",
        "gap": {"reason": "item_exceeds_single_item_bytes"},
    });
    assert_eq!(
        matched_categories(&all, &gap, "operator"),
        [Category::Reports, Category::Operations]
    );
    // Category subsets are respected exactly.
    assert_eq!(
        matched_categories(&[Category::Mailbox], &mail, "operator"),
        [Category::Mailbox]
    );
    assert!(matched_categories(&[Category::Operations], &native, "operator").is_empty());
}

#[test]
fn subscribe_params_are_strict() {
    let (categories, after) = parse_subscribe(&json!({
        "categories": ["reports", "operations", "concilium", "reports"],
        "after": 12,
    }))
    .unwrap();
    assert_eq!(
        categories,
        [Category::Reports, Category::Operations, Category::Concilium]
    );
    assert_eq!(after, Some(12));
    let (_, after) = parse_subscribe(&json!({"categories": ["mailbox"]})).unwrap();
    assert_eq!(after, None);
    for bad in [
        json!({}),
        json!({"categories": []}),
        json!({"categories": ["reports", "live-stream"]}),
        json!({"categories": [42]}),
        json!({"categories": ["reports"], "after": -1}),
        json!({"categories": ["reports"], "after": "12"}),
    ] {
        let error = parse_subscribe(&bad).unwrap_err();
        assert_eq!(error.code, "INVALID_PARAMS", "{bad}");
    }
    assert_eq!(
        parse_unsubscribe(&json!({"subscription_id": "s-1"})).unwrap(),
        "s-1"
    );
    for bad in [
        json!({}),
        json!({"subscription_id": ""}),
        json!({"subscription_id": 3}),
    ] {
        assert_eq!(parse_unsubscribe(&bad).unwrap_err().code, "INVALID_PARAMS");
    }
}

#[test]
fn notification_shapes_carry_frame_identity_and_resync() {
    let frame = json!({
        "source_kind": "observation_timeline",
        "projection_revision": "sha256:abc",
        "range": {"after": 3, "next_cursor": 7},
        "coverage_complete": true,
        "gap_reason": null,
    });
    let entry = item("task.create", Some("op-1"), json!({"task_id": "t-1"}));
    let notification = committed_notification(
        "sub-1",
        &[Category::Reports, Category::Operations],
        &entry,
        &frame,
    );
    assert_eq!(notification.method, COMMITTED_NOTIFICATION);
    let params = notification.params.expect("params");
    assert_eq!(params["subscription_id"], json!("sub-1"));
    assert_eq!(params["categories"], json!(["reports", "operations"]));
    assert_eq!(params["cursor"], json!(7));
    assert_eq!(params["item"], entry);
    // The frame is carried verbatim -- the subscriber detects gaps
    // from the page's own range/revision, not from trust.
    assert_eq!(params["frame"], frame);

    let gap = LaggedGap {
        dropped_items: 6,
        from_cursor: 4,
        through_cursor: 10,
        head_reached: true,
    };
    let lagged = lagged_notification("sub-1", &gap);
    assert_eq!(lagged.method, LAGGED_NOTIFICATION);
    let params = lagged.params.expect("params");
    assert_eq!(params["dropped_items"], json!(6));
    assert_eq!(params["from_cursor"], json!(4));
    assert_eq!(params["through_cursor"], json!(10));
    assert_eq!(params["resync"]["after"], json!(4));
    assert_eq!(
        params["resync"]["reads"],
        json!(["report.delta", "message.read", "operation.get"])
    );
}
