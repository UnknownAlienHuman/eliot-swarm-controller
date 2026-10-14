use super::{PROJECT, call, claim, create, spec, start, write};
use crate::store::projection::{
    GAP_ITEM_EXCEEDS_SINGLE, MAX_PROJECTED_ITEMS, MAX_SINGLE_ITEM_BYTES,
};
use crate::{error::Error, model};
use rusqlite::params;
use serde_json::{Value, json};

const DELTA_CONTINUATION_SLACK: usize = 8;
const VISIBLE_BLOCKS: usize = 2;

fn canonical_text(value: &Value) -> String {
    model::canonical(value).unwrap()
}

fn assert_foreign_ids_absent(value: &Value, task_ids: &[String], operation_ids: &[String]) {
    let rendered = canonical_text(value);
    for hidden in task_ids.iter().chain(operation_ids) {
        assert!(
            !rendered.contains(hidden.as_str()),
            "foreign retained ID escaped a public gap/page: {hidden} in {rendered}"
        );
    }
}

#[tokio::test]
async fn store_authenticated_task_list_delta_bounded_gap_tail_recovery() {
    let fixture = start().await;
    let store = &fixture.owner.store;
    let scan_limit = MAX_PROJECTED_ITEMS;
    let foreign_per_block = scan_limit / VISIBLE_BLOCKS;
    assert_eq!(scan_limit % VISIBLE_BLOCKS, 0);
    let mut visible_tasks = Vec::with_capacity(VISIBLE_BLOCKS);
    let mut foreign_tasks = Vec::with_capacity(scan_limit);
    let mut foreign_operation_ids = Vec::with_capacity(scan_limit + 2);

    // Interleave real A-owned Tasks with enough real B-owned Tasks to fill the
    // production scan window. The later large Task and tail remain beyond it.
    for block in 0..VISIBLE_BLOCKS {
        let visible_task = create(
            store,
            &fixture.manager_a,
            &format!("bounded-gap-visible-{block}"),
        )
        .await;
        claim(store, &fixture.manager_a, &visible_task, None).await;
        visible_tasks.push(visible_task);
        for slot in 0..foreign_per_block {
            let index = block * foreign_per_block + slot;
            let task = create(
                store,
                &fixture.manager_b,
                &format!("bounded-gap-foreign-{index}"),
            )
            .await;
            foreign_operation_ids.push(
                task["operation_id"]
                    .as_str()
                    .expect("task.create returns its Operation ID")
                    .to_owned(),
            );
            foreign_tasks.push(task);
        }
    }

    let foreign_claim = claim(store, &fixture.manager_b, &foreign_tasks[0], None).await;
    let claim_operation_id = foreign_claim["operation_id"].as_str().unwrap().to_owned();
    let claim_attempt_id = foreign_claim["attempt_id"].as_str().unwrap().to_owned();
    foreign_operation_ids.push(claim_operation_id);
    let damaged_task = create(store, &fixture.manager_b, "bounded-gap-damaged-operation").await;
    let damaged_task_id = damaged_task["task_id"].as_str().unwrap().to_owned();
    let damage_operation_id = damaged_task["operation_id"].as_str().unwrap().to_owned();
    foreign_operation_ids.push(damage_operation_id.clone());

    // Preserve real, existing IDs while attaching a real Attempt from another
    // Task to this existing task.create Operation. Both columns remain valid
    // strings; only their retained relation is damaged, which must become an
    // opaque public delta gap.
    let update_operation_id = damage_operation_id.clone();
    let update_attempt_id = claim_attempt_id.clone();
    let update_task_id = damaged_task_id.clone();
    let changed = store
        .run_fixture(move |db| {
            Ok(db.execute(
                "UPDATE operations SET attempt_id=?1 WHERE operation_id=?2 AND task_id=?3 AND attempt_id IS NULL",
                params![update_attempt_id, update_operation_id, update_task_id],
            )?)
        })
        .await
        .unwrap();
    assert_eq!(changed, 1);

    let caller_id = fixture.manager_b.client_id.clone();
    let (first_damage_cursor, foreign_timeline_rows) = store
        .run_fixture(move |db| {
            let first_damage_cursor: Option<i64> = db.query_row(
                "SELECT MIN(observation_id) FROM observations \
                 WHERE operation_id=?1 AND source_stream_id NOT IN \
                   ('controller:messages','controller:read-position:operation','controller:read-position:task')",
                [damage_operation_id],
                |row| row.get(0),
            )?;
            let first_damage_cursor = first_damage_cursor.ok_or_else(|| {
                Error::new("TEST_FIXTURE", "damaged task.create has no timeline observation")
            })?;
            let foreign_timeline_rows: i64 = db.query_row(
                "SELECT COUNT(*) FROM observations AS o \
                 JOIN operations AS op ON op.operation_id=o.operation_id \
                 WHERE op.caller_id=?1 AND op.method='task.create' \
                   AND o.observation_id<?2 \
                   AND o.source_stream_id NOT IN \
                     ('controller:messages','controller:read-position:operation','controller:read-position:task')",
                params![caller_id, first_damage_cursor],
                |row| row.get(0),
            )?;
            Ok((first_damage_cursor, foreign_timeline_rows))
        })
        .await
        .unwrap();
    assert!(
        foreign_timeline_rows >= scan_limit as i64,
        "fixture must place at least one full production scan of foreign Operations before the damaged row: {foreign_timeline_rows}"
    );

    // A valid TaskSpec with a large but schema-permitted objective makes one
    // authorized task.list projection exceed the production single-item cap.
    let large_objective = "x".repeat(MAX_SINGLE_ITEM_BYTES / 2 + 1_024);
    let mut large_spec = spec("bounded-gap-large-task");
    large_spec["objective"] = json!(large_objective.clone());
    let oversized_task = write(
        store,
        &fixture.manager_a,
        "task.create",
        json!({
            "project_id": PROJECT,
            "origin_key": "scope-origin-bounded-gap-large-task",
            "spec": large_spec,
        }),
    )
    .await;
    claim(store, &fixture.manager_a, &oversized_task, None).await;
    let tail_task = create(store, &fixture.manager_a, "bounded-gap-tail").await;
    claim(store, &fixture.manager_a, &tail_task, None).await;
    let oversized_task_id = oversized_task["task_id"].as_str().unwrap().to_owned();
    let tail_task_id = tail_task["task_id"].as_str().unwrap().to_owned();
    let tail_operation_id = tail_task["operation_id"].as_str().unwrap().to_owned();
    let mut foreign_task_ids: Vec<String> = foreign_tasks
        .iter()
        .map(|task| task["task_id"].as_str().unwrap().to_owned())
        .collect();
    foreign_task_ids.push(damaged_task_id);

    let scan_limit_i64 = scan_limit as i64;
    let expected_task_scan_cursor = store
        .run_fixture(move |db| {
            Ok(db.query_row(
                "SELECT pos.observation_id FROM observations AS pos \
                 JOIN tasks AS target ON target.task_id=pos.source_event_key \
                 WHERE pos.source_stream_id='controller:read-position:task' \
                   AND pos.observation_id>0 \
                 ORDER BY pos.observation_id LIMIT 1 OFFSET ?1",
                [scan_limit_i64 - 1],
                |row| row.get::<_, i64>(0),
            )?)
        })
        .await
        .unwrap();
    // The first public Task page scans 200 interleaved A/B Task rows and reaches
    // the real scan bound. The next page emits the oversized Task as a gap and
    // leaves the later authorized Task for cursor-tail recovery.
    let first_task_page = call(
        store,
        &fixture.manager_a,
        "task.list",
        json!({"after":0,"limit":scan_limit}),
    )
    .await;
    let first_task_items = first_task_page["items"].as_array().unwrap();
    let actual_visible_ids: Vec<String> = first_task_items
        .iter()
        .map(|item| item["task_id"].as_str().unwrap().to_owned())
        .collect();
    let expected_visible_ids: Vec<String> = visible_tasks
        .iter()
        .map(|task| task["task_id"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(actual_visible_ids, expected_visible_ids);
    assert!(first_task_page.get("filtered_count").is_none());
    assert!(first_task_page["has_newer"].as_bool().unwrap());
    assert!(first_task_page["coverage_complete"].as_bool().unwrap());
    assert_eq!(first_task_page["gap_count"], 0);
    let task_scan_cursor = first_task_page["next_after"].as_i64().unwrap();
    assert!(task_scan_cursor > 0);
    assert_eq!(task_scan_cursor, expected_task_scan_cursor);
    assert_foreign_ids_absent(&first_task_page, &foreign_task_ids, &foreign_operation_ids);

    let task_gap_page = call(
        store,
        &fixture.manager_a,
        "task.list",
        json!({"after":task_scan_cursor,"limit":1}),
    )
    .await;
    assert_eq!(task_gap_page["items"].as_array().unwrap().len(), 1);
    let task_gap = &task_gap_page["items"][0];
    assert_eq!(task_gap["task_id"], oversized_task_id);
    assert_eq!(task_gap["gap"]["reason"], GAP_ITEM_EXCEEDS_SINGLE);
    assert_eq!(task_gap["gap"]["read_method"], "task.get");
    assert!(
        task_gap["gap"]["serialized_byte_length"].as_u64().unwrap() > MAX_SINGLE_ITEM_BYTES as u64
    );
    assert!(!task_gap_page["coverage_complete"].as_bool().unwrap());
    assert_eq!(task_gap_page["gap_count"], 1);
    assert!(task_gap_page.get("filtered_count").is_none());
    assert!(task_gap_page["has_newer"].as_bool().unwrap());
    assert_eq!(task_gap_page["next_after"], task_gap["cursor"]);
    assert!(!canonical_text(&task_gap_page).contains(&large_objective));
    assert_foreign_ids_absent(&task_gap_page, &foreign_task_ids, &foreign_operation_ids);

    let task_tail_page = call(
        store,
        &fixture.manager_a,
        "task.list",
        json!({"after":task_gap_page["next_after"],"limit":1}),
    )
    .await;
    assert_eq!(task_tail_page["items"][0]["task_id"], tail_task_id);
    assert!(task_tail_page["coverage_complete"].as_bool().unwrap());
    assert_eq!(task_tail_page["gap_count"], 0);
    assert_foreign_ids_absent(&task_tail_page, &foreign_task_ids, &foreign_operation_ids);

    // The same interleaved real Operation observations fill one public delta scan.
    // Its retained Task/Attempt mismatch then produces an opaque gap, and the
    // cursor must eventually deliver the later authorized Task operation.
    let first_delta_page = call(
        store,
        &fixture.manager_a,
        "report.delta",
        json!({"after":0,"limit":scan_limit}),
    )
    .await;
    assert_eq!(
        first_delta_page["projection"]["limits"]["max_source_rows"],
        (scan_limit + 1) as i64
    );
    assert!(
        first_delta_page["projection"]["has_newer"]
            .as_bool()
            .unwrap()
    );
    assert!(first_delta_page["items"].as_array().unwrap().len() < scan_limit);
    assert!(
        first_delta_page["projection"]["coverage_complete"]
            .as_bool()
            .unwrap()
    );
    assert_eq!(first_delta_page["projection"]["gap_count"], 0);
    assert!(
        first_delta_page["projection"]["range"]
            .get("filtered_count")
            .is_none()
    );
    let mut delta_cursor = first_delta_page["next_cursor"].as_i64().unwrap();
    assert!(delta_cursor > 0);
    assert!(delta_cursor < first_damage_cursor);
    assert_foreign_ids_absent(&first_delta_page, &foreign_task_ids, &foreign_operation_ids);

    let max_continuation_pages = foreign_timeline_rows as usize / scan_limit
        + (visible_tasks.len() + 2) * 2
        + DELTA_CONTINUATION_SLACK;
    let mut damage_gap_cursor = None;
    let mut tail_recovered = false;
    for _ in 0..max_continuation_pages {
        let page = call(
            store,
            &fixture.manager_a,
            "report.delta",
            json!({"after":delta_cursor,"limit":1}),
        )
        .await;
        assert_eq!(
            page["projection"]["limits"]["max_source_rows"],
            (scan_limit + 1) as i64
        );
        assert!(page["projection"]["range"].get("filtered_count").is_none());
        assert_foreign_ids_absent(&page, &foreign_task_ids, &foreign_operation_ids);
        let items = page["items"].as_array().unwrap();
        if page["projection"]["gap_count"].as_u64().unwrap_or_default() > 0 {
            assert!(!page["projection"]["coverage_complete"].as_bool().unwrap());
            assert_eq!(page["projection"]["gap_count"], 1);
        }
        if let Some(gap) = items
            .iter()
            .find(|item| item["gap"]["error_code"] == "OBJECT_SCOPE_DAMAGED")
        {
            assert_eq!(gap["gap"]["reason"], "retained_relation_damaged");
            assert_eq!(page["projection"]["coverage_complete"], false);
            assert_eq!(page["projection"]["gap_count"], 1);
            assert_eq!(
                page["projection"]["gap_reason"],
                "retained_relation_damaged"
            );
            assert!(page["projection"]["has_newer"].as_bool().unwrap());
            assert!(!canonical_text(&page).contains(&large_objective));
            assert!(gap.get("operation_id").is_none());
            assert!(gap.get("task_id").is_none());
            assert_eq!(page["next_cursor"], gap["cursor"]);
            damage_gap_cursor = Some(gap["cursor"].as_i64().unwrap());
        }
        if items
            .iter()
            .any(|item| item["operation_id"].as_str() == Some(tail_operation_id.as_str()))
        {
            let gap_cursor =
                damage_gap_cursor.expect("delta reached the damaged foreign row first");
            assert!(page["next_cursor"].as_i64().unwrap() > gap_cursor);
            tail_recovered = true;
            break;
        }
        let next = page["next_cursor"].as_i64().unwrap();
        assert!(
            next > delta_cursor,
            "delta cursor stalled before the tail row"
        );
        delta_cursor = next;
    }
    assert!(
        damage_gap_cursor.is_some(),
        "public delta never emitted the damaged relation gap"
    );
    assert!(
        tail_recovered,
        "public delta cursor did not recover the later authorized Operation"
    );

    fixture.close().await;
}
