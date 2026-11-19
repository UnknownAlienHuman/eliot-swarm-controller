//! The canonical Agent row parser extracted from Store. Event filters retain
//! their separate nonempty-string grammar and do not use this parser.

use crate::{Error, Result};

/// Parse the exact `a<positive i64>` representation without normalization.
pub fn parse_agent_id(id: &str) -> Result<i64> {
    let digits = id
        .strip_prefix('a')
        .ok_or_else(|| Error::new("invalid", "Agent id must use the a<positive integer> form"))?;
    let number = digits
        .parse::<i64>()
        .map_err(|_| Error::new("invalid", "Agent id is not a signed 64-bit integer"))?;
    if number <= 0 || format!("a{number}") != id {
        return Err(Error::new(
            "invalid",
            "Agent id must be canonical and positive",
        ));
    }
    Ok(number)
}
