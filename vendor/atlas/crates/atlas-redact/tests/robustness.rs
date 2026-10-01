//! Robustness: redaction sits on the turn-completion path, so it must never be
//! the thing that loses a turn.
//!
//! Nothing here may panic, and nothing may take long enough to be felt. The
//! inputs are the ones a real agent transcript actually produces: a 40 MB tool
//! result, a single line with no newlines in it, replacement characters from a
//! binary file the agent tried to read, deeply nested JSON from a tool schema.

use atlas_redact::{redact, redact_json, PLACEHOLDER};

mod fixtures;

#[test]
fn redacting_already_redacted_content_is_a_no_op() {
    let inputs = [
        "API_KEY=supersecretvalue123".to_string(),
        "postgres://app:hunter2@db.internal:5432/prod".to_string(),
        "host=db user=app password=hunter2 sslmode=require".to_string(),
        format!("the key is {} here", fixtures::anthropic_api_key()),
        fixtures::opaque_high_entropy_token(),
    ];
    for input in inputs {
        let input = input.as_str();
        let once = redact(input);
        let twice = redact(&once.text);
        assert_eq!(
            twice.text, once.text,
            "second pass changed the output for {input:?}"
        );
        assert_eq!(
            twice.counts.total(),
            0,
            "second pass re-redacted its own placeholder for {input:?}"
        );
    }
}

#[test]
fn json_redaction_is_idempotent_too() {
    let inputs = [
        r#"{"content":"API_KEY=supersecretvalue123","client_secret":"hunter2xyz"}"#,
        // The link-preserving url path: only the password is replaced, and the
        // replacement must be invisible to a second pass.
        r#"{"url":"postgres://app:hunter2@db/x"}"#,
        r#"{"uri":"mysql://db.internal/app?user=svc&password=hunter2"}"#,
        // The unconditional bare-password key.
        r#"{"password":"hunter2"}"#,
    ];
    for input in inputs {
        let once = redact_json(input);
        let twice = redact_json(&once.text);
        assert_eq!(twice.text, once.text, "second pass changed the output for {input:?}");
        assert_eq!(twice.counts.total(), 0, "second pass re-redacted for {input:?}");
    }
}

#[test]
fn flat_redaction_of_json_fragments_is_idempotent() {
    // The quoted-key credential form, exercised through the flat pass — a JSON
    // fragment inside prose never reaches the walker.
    let inputs = [
        r#"the tool sent {"password": "hunter2"} and failed"#,
        r#"config was password: "hunter2" at the time"#,
    ];
    for input in inputs {
        let once = redact(input);
        assert!(!once.text.contains("hunter2"), "secret survived: {}", once.text);
        let twice = redact(&once.text);
        assert_eq!(twice.text, once.text, "second pass changed the output for {input:?}");
        assert_eq!(twice.counts.total(), 0, "second pass re-redacted for {input:?}");
    }
}

#[test]
fn non_utf8_bytes_are_declined_rather_than_mangled() {
    // A tool result that is a compiled binary, an image, or a truncated
    // multi-byte read. Scanning it would mean lossy-decoding, which corrupts the
    // payload on the way back out for no benefit — there is no secret to be
    // found in bytes that cannot be read as text.
    assert!(atlas_redact::redact_bytes(&[0xff, 0xfe, 0x00, 0x41]).is_none());
    assert!(atlas_redact::redact_bytes(&[0xc3, 0x28]).is_none());

    // Valid UTF-8 bytes still go through the normal path.
    let out = atlas_redact::redact_bytes(b"API_KEY=supersecretvalue123").unwrap();
    assert!(out.text.contains(PLACEHOLDER));
}

#[test]
fn replacement_characters_from_a_binary_read_do_not_panic() {
    // A tool that reads a binary file and lossily decodes it produces exactly
    // this: valid UTF-8 littered with U+FFFD.
    let lossy = String::from_utf8_lossy(&[0xff, 0xfe, 0x00, 0x41, 0x80, 0x42, 0xc3, 0x28]);
    let out = redact(&lossy);
    assert!(!out.text.is_empty());
}

#[test]
fn multibyte_text_is_never_sliced_mid_character() {
    for input in [
        "こんにちは、セッションを記録しています",
        "naïve café résumé — em dash and ellipsis…",
        "🔐 the key is xJ3kQ9vB2mZ7pL5rT8wN4cF6yH1sD0gAe4Ru 🔐",
        "clé_secrète=très-long-mot-de-passe-ici",
    ] {
        let out = redact(input);
        // The invariant is that the output is still valid UTF-8 that round-trips.
        assert_eq!(out.text, String::from_utf8(out.text.clone().into_bytes()).unwrap());
    }
}

#[test]
fn an_extremely_long_single_line_completes() {
    // The measured corpus has a single 2.02 MB message. A tool result is larger
    // still, and neither arrives with helpful line breaks.
    let line = "word ".repeat(400_000);
    let started = std::time::Instant::now();
    let out = redact(&line);
    assert_eq!(out.counts.total(), 0);
    assert!(
        started.elapsed().as_secs() < 20,
        "took {:?} — too slow for the turn-completion path",
        started.elapsed()
    );
}

#[test]
fn a_secret_at_the_very_end_of_a_long_line_is_still_found() {
    let mut line = "harmless ".repeat(50_000);
    line.push_str("API_KEY=supersecretvalue123");
    let out = redact(&line);
    assert!(!out.text.contains("supersecretvalue123"));
}

#[test]
fn deeply_nested_json_does_not_blow_the_stack() {
    // serde_json's own recursion limit rejects pathological nesting before we
    // ever walk it; what matters is that we return rather than abort.
    let depth = 5_000;
    let nested = format!(
        "{}{}{}",
        "{\"a\":".repeat(depth),
        "\"API_KEY=supersecretvalue123\"",
        "}".repeat(depth)
    );
    let out = redact_json(&nested);
    assert!(!out.text.contains("supersecretvalue123"), "secret survived");
}

#[test]
fn moderately_nested_json_is_walked_leaf_by_leaf() {
    let depth = 40;
    let nested = format!(
        "{}{}{}",
        "{\"a\":".repeat(depth),
        "\"API_KEY=supersecretvalue123\"",
        "}".repeat(depth)
    );
    let out = redact_json(&nested);
    assert!(out.text.contains(PLACEHOLDER));
    assert!(out.text.starts_with("{\"a\":{\"a\":"), "structure lost");
}

#[test]
fn malformed_jsonl_lines_are_still_scrubbed_rather_than_skipped() {
    let input = "{\"content\":\"hello\"}\nnot json at all API_KEY=supersecretvalue123\n{\"content\":\"bye\"}";
    let out = redact_json(input);
    assert!(!out.text.contains("supersecretvalue123"));
    assert!(out.text.contains("hello") && out.text.contains("bye"));
    assert_eq!(out.text.lines().count(), 3, "line structure lost");
}

#[test]
fn a_truncated_final_jsonl_line_does_not_lose_the_earlier_lines() {
    // A transcript being appended to while it is read ends mid-object.
    let input = "{\"content\":\"first\"}\n{\"content\":\"second\"}\n{\"content\":\"thi";
    let out = redact_json(input);
    assert!(out.text.contains("first") && out.text.contains("second"));
    assert!(out.text.contains("thi"));
}

#[test]
fn an_empty_or_whitespace_input_is_returned_unchanged() {
    for input in ["", "   ", "\n\n", "\t"] {
        assert_eq!(redact(input).text, input);
        assert_eq!(redact_json(input).text, input);
    }
}
