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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_positive_agent_ids_include_the_i64_boundary() {
        for (id, number) in [
            ("a1", 1),
            ("a9", 9),
            ("a10", 10),
            ("a9223372036854775807", i64::MAX),
        ] {
            assert_eq!(parse_agent_id(id).unwrap(), number);
        }
    }

    #[test]
    fn rejected_agent_id_forms_keep_the_existing_error_codes_and_messages() {
        for id in ["", "1", "A1", "b1", " a1", "é1"] {
            let error = parse_agent_id(id).unwrap_err();
            assert_eq!(error.code, "invalid");
            assert_eq!(
                error.message,
                "Agent id must use the a<positive integer> form"
            );
        }
        for id in [
            "a",
            "a1 ",
            "a 1",
            "a1\n",
            "a١",
            "a１",
            "a1.0",
            "a--1",
            "a9223372036854775808",
            "a-9223372036854775809",
        ] {
            let error = parse_agent_id(id).unwrap_err();
            assert_eq!(error.code, "invalid");
            assert_eq!(error.message, "Agent id is not a signed 64-bit integer");
        }
        for id in [
            "a0",
            "a00",
            "a01",
            "a+1",
            "a-0",
            "a-1",
            "a-9223372036854775808",
        ] {
            let error = parse_agent_id(id).unwrap_err();
            assert_eq!(error.code, "invalid");
            assert_eq!(error.message, "Agent id must be canonical and positive");
        }
    }
}
