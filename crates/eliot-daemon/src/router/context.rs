//! Runtime-only context attachment for the single original send command.

use super::{Command, Error, Result};
use eliot_core::ContextItem;

/// Copy claimed context onto the transient plugin command args. The retained
/// Store command and its idempotency payload stay unchanged.
pub(super) fn with_next_input(command: &Command, items: &[ContextItem]) -> Result<Command> {
    if items.is_empty() {
        return Ok(command.clone());
    }
    if command.verb != "send" {
        return Err(Error::new(
            "source_integrity",
            "next_input context was returned for a non-send command",
        ));
    }
    let mut command = command.clone();
    let args = command
        .args
        .as_object_mut()
        .ok_or_else(|| Error::new("source_integrity", "send args are not an object"))?;
    if args.contains_key("context_items") {
        return Err(Error::new(
            "source_integrity",
            "stored send args contain the reserved context_items member",
        ));
    }
    args.insert(
        "context_items".to_owned(),
        serde_json::to_value(items)
            .map_err(|_| Error::new("internal", "context items could not be encoded"))?,
    );
    Ok(command)
}
