use super::*;
use std::sync::{Barrier, atomic::Ordering};

// Fresh descriptors use the production OnceLock seam without resetting global
// slots or depending on the order in which other tests discover tools.
fn fresh_spec(method: &'static str) -> (bool, ToolSpec) {
    let (read_only, spec) = find_tool_spec(method).expect("registered fixture method");
    if *read_only {
        read(method, spec.description, spec.fields, spec.required)
    } else {
        mutation(method, spec.description, spec.fields, spec.required)
    }
}

fn counts(spec: &ToolSpec) -> [usize; 6] {
    std::array::from_fn(|index| spec.factory_counts[index].load(Ordering::Relaxed))
}

#[test]
fn schema_cache_exact_cold_and_warm_factory_counts() {
    let (read_only, spec) = fresh_spec("message.send");
    assert_eq!(counts(&spec), [0; 6]);
    let optional = input_schema_bytes(&spec, read_only, false).expect("optional bytes");
    let required = input_schema_bytes(&spec, read_only, true).expect("required bytes");
    let output = output_schema_bytes_for_spec(&spec)
        .expect("output bytes")
        .expect("mailbox output schema");
    // Input factory x2, input serialization x2, output factory x1,
    // output serialization x1. Request-shaped data never selects a slot.
    assert_eq!(counts(&spec), [1; 6]);
    assert_ne!(optional.as_ref(), required.as_ref());
    assert_ne!(required.as_ref(), output.as_ref());
    for _ in 0..64 {
        assert!(Arc::ptr_eq(
            &optional,
            &input_schema_bytes(&spec, read_only, false).expect("warm optional")
        ));
        assert!(Arc::ptr_eq(
            &required,
            &input_schema_bytes(&spec, read_only, true).expect("warm required")
        ));
        assert!(Arc::ptr_eq(
            &output,
            &output_schema_bytes_for_spec(&spec)
                .expect("warm output")
                .expect("output schema")
        ));
        assert_eq!(
            serde_json::to_vec(input_schema(&spec, read_only, true).as_ref())
                .expect("direct input serialization"),
            required.as_ref()
        );
        assert_eq!(
            serde_json::to_vec(
                output_schema_for_spec(&spec)
                    .expect("direct output")
                    .as_ref()
            )
            .expect("direct output serialization"),
            output.as_ref()
        );
    }
    assert_eq!(counts(&spec), [1; 6], "warm lookups build nothing");
}

#[test]
fn schema_cache_concurrent_first_use_initializes_each_slot_once() {
    let (read_only, spec) = fresh_spec("message.send");
    let barrier = Barrier::new(8);
    let retained = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let spec = &spec;
                let barrier = &barrier;
                scope.spawn(move || {
                    barrier.wait();
                    (
                        input_schema(spec, read_only, true),
                        input_schema_bytes(spec, read_only, true).expect("input bytes"),
                        output_schema_for_spec(spec).expect("output schema"),
                        output_schema_bytes_for_spec(spec)
                            .expect("output bytes")
                            .expect("output schema"),
                    )
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().expect("lookup thread"))
            .collect::<Vec<_>>()
    });
    assert_eq!(counts(&spec), [0, 1, 0, 1, 1, 1]);
    for value in &retained[1..] {
        assert!(Arc::ptr_eq(&retained[0].0, &value.0));
        assert!(Arc::ptr_eq(&retained[0].1, &value.1));
        assert!(Arc::ptr_eq(&retained[0].2, &value.2));
        assert!(Arc::ptr_eq(&retained[0].3, &value.3));
    }
}

#[test]
fn schema_cache_absent_output_is_retained_and_unknown_lookup_fails_closed() {
    let (_, spec) = fresh_spec("host.status");
    for _ in 0..16 {
        assert!(output_schema_for_spec(&spec).is_none());
        assert!(
            output_schema_bytes_for_spec(&spec)
                .expect("absent output")
                .is_none()
        );
        for method in ["", "unknown.method", "HOST.STATUS", "host.status "] {
            assert!(find_tool_spec(method).is_none());
            assert!(output_schema(method).is_none());
            assert!(
                output_schema_bytes(method)
                    .expect("unknown output")
                    .is_none()
            );
        }
        assert!(serde_json::from_str::<McpToolProfile>("\"unknown-profile\"").is_err());
    }
    assert_eq!(counts(&spec), [0, 0, 0, 0, 1, 1]);
}
#[test]
fn schema_cache_registry_cold_and_warm_cost_is_bounded_by_static_slots() {
    validate_registry_metadata().expect("static catalog validates before factory accounting");
    let mut factory_calls = 0usize;
    for metadata in TOOL_METADATA {
        let (read_only, spec) = fresh_spec(metadata.method);
        let _optional = input_schema_bytes(&spec, read_only, false).expect("optional bytes");
        let _required = input_schema_bytes(&spec, read_only, true).expect("required bytes");
        let _output = output_schema_bytes_for_spec(&spec).expect("output bytes");
        assert_eq!(counts(&spec), [1; 6], "cold slots: {}", metadata.method);
        factory_calls += counts(&spec).into_iter().sum::<usize>();
        for _ in 0..8 {
            let _ = input_schema_bytes(&spec, read_only, false).expect("warm optional");
            let _ = input_schema_bytes(&spec, read_only, true).expect("warm required");
            let _ = output_schema_bytes_for_spec(&spec).expect("warm output");
        }
        assert_eq!(counts(&spec), [1; 6], "warm slots: {}", metadata.method);
    }
    assert_eq!(factory_calls, TOOL_METADATA.len() * 6);
    println!(
        "PR40_SCHEMA_FACTORY_COST static_methods={} slots_per_method=6 cold_factory_calls={} warm_factory_calls=0",
        TOOL_METADATA.len(),
        factory_calls,
    );
}
