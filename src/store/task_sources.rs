//! Deterministic Task source-index projections shared by Task reads and Attempt snapshots.

use crate::model::TaskSpec;
use serde_json::{Value, json};

pub(super) fn brief(spec: &TaskSpec) -> Value {
    json!(spec.brief())
}

pub(in crate::store) fn project_brief(raw_spec: &Value) -> Value {
    let Ok(spec) = serde_json::from_value::<TaskSpec>(raw_spec.clone()) else {
        return unavailable_brief();
    };
    brief(&spec)
}

fn unavailable_brief() -> Value {
    json!({
        "status": "unavailable",
        "reason": "stored_task_spec_unreadable",
    })
}
