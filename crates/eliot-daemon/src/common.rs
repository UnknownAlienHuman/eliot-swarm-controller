//! Shared daemon primitives. Wall-clock timestamps are not deadline clocks.

use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

pub(crate) fn secure_eq(left: &str, right: &str) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.bytes()
        .zip(right.bytes())
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

/// Syntax only: trimming, bounds, reserved names and platform key policy belong
/// to callers.
pub(crate) fn valid_environment_name(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some('_' | 'A'..='Z' | 'a'..='z'))
        && chars.all(|character| character == '_' || character.is_ascii_alphanumeric())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_helpers_secure_eq_preserves_byte_and_length_behavior() {
        for (left, right) in [
            ("", ""),
            ("credential", "credential"),
            ("é", "é"),
            ("a\0b", "a\0b"),
        ] {
            assert!(secure_eq(left, right));
        }
        for (left, right) in [
            ("", "a"),
            ("a", ""),
            ("token", "token!"),
            ("token", "Token"),
            ("token", "tokeN"),
            ("é", "aa"),
            ("é", "ê"),
            ("a\0b", "a\0c"),
        ] {
            assert!(!secure_eq(left, right));
        }
    }

    #[test]
    fn shared_helpers_environment_names_remain_syntax_only() {
        for name in [
            "_",
            "__a9",
            "a0",
            "PATH",
            "mixed_Case123",
            "ELIOT_AGENT_TOKEN",
        ] {
            assert!(valid_environment_name(name));
        }
        assert!(valid_environment_name(&format!("A{}", "x".repeat(256))));
        for name in [
            "", "9A", "a-b", " A", "A ", "A=B", "A.B", "A\0", "é", "Aé", "A\n",
        ] {
            assert!(!valid_environment_name(name));
        }
    }
}
