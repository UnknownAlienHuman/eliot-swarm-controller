use super::{
    AuthorizationBasis, AuthorizationRevision, CatalogError, SearchRequest, Surface,
    list_tools_page, reset_search_view_builds, search_catalog, search_view_builds,
};
use crate::config::McpToolProfile;
use sha2::Digest;
use swarm_contracts::mcp_catalog::{ToolAudience, metadata_for};

#[test]
fn catalog_direct_schema_bytes_and_profile_membership_match() {
    use swarm_contracts::mcp_catalog::{
        TOOL_METADATA, input_schema, input_schema_bytes, mutation_requires_caller_request_id,
        output_schema, output_schema_bytes, tool_name,
    };

    let profiles = [
        McpToolProfile::Observer,
        McpToolProfile::Reviewer,
        McpToolProfile::Participant,
        McpToolProfile::AssignedReviewer,
        McpToolProfile::Manager,
        McpToolProfile::Gm,
        McpToolProfile::Full,
    ];
    let authorization = AuthorizationRevision {
        value: "catalog-parity-v1",
        basis: AuthorizationBasis::AuthenticatedStoreScope,
    };
    for profile in profiles {
        // Expose all presentation entries while preserving the hard profile.
        let mut surface = Surface::role_default(profile);
        surface.legacy_full = true;
        let mut expected: Vec<_> = TOOL_METADATA
            .iter()
            .filter(|entry| super::profiles::exposes_method(profile, entry.method))
            .collect();
        expected.sort_by(|left, right| super::metadata_order(left, right));
        let mut listed = Vec::new();
        let mut cursor = None;
        loop {
            let page = list_tools_page(
                profile,
                &surface,
                cursor.as_deref(),
                authorization,
                |_, _| true,
            )
            .expect("profile catalog page");
            listed.extend(page.tools);
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(listed.len(), expected.len(), "{profile:?} membership");
        for (tool, entry) in listed.iter().zip(expected) {
            let (read_only, spec) = super::find_spec(entry.method).expect("ToolSpec");
            let required = mutation_requires_caller_request_id(profile, spec.method, *read_only);
            let wire = serde_json::to_value(tool).expect("wire tool");
            assert_eq!(wire["name"], tool_name(spec.method), "method order");
            assert_eq!(wire["description"], spec.description);
            let input = input_schema_bytes(spec, *read_only, required).expect("cached input");
            let catalog_input = serde_json::to_vec(&wire["inputSchema"]).expect("catalog input");
            assert_eq!(catalog_input, input.as_ref(), "{} {profile:?}", spec.method);
            assert_eq!(
                serde_json::to_vec(input_schema(spec, *read_only, required).as_ref())
                    .expect("direct input"),
                input.as_ref()
            );
            assert_eq!(
                super::hex_digest(&super::Sha256::digest(&catalog_input)),
                super::hex_digest(&super::Sha256::digest(input.as_ref()))
            );
            match output_schema_bytes(spec.method).expect("cached output") {
                Some(output) => {
                    assert_eq!(
                        serde_json::to_vec(&wire["outputSchema"]).expect("catalog output"),
                        output.as_ref()
                    );
                    assert_eq!(
                        serde_json::to_vec(
                            output_schema(spec.method).expect("direct output").as_ref()
                        )
                        .expect("direct output bytes"),
                        output.as_ref()
                    );
                    assert_ne!(input.as_ref(), output.as_ref(), "input/output slots");
                }
                None => assert!(wire.get("outputSchema").is_none()),
            }
            // Restricted profiles share one required-ID variant. Full's
            // optional-ID compatibility variant is an intentional distinction.
            if profile != McpToolProfile::Full {
                let full_required = mutation_requires_caller_request_id(
                    McpToolProfile::Full,
                    spec.method,
                    *read_only,
                );
                if required == full_required {
                    let full = super::super::tool_from_spec(*read_only, spec, full_required);
                    assert_eq!(
                        serde_json::to_value(full).expect("full wire")["inputSchema"],
                        wire["inputSchema"]
                    );
                }
            }
        }
    }
}

#[test]
fn exact_search_keeps_full_view_revisions_and_loaded_revision_checks() {
    let profile = McpToolProfile::Full;
    let surface = Surface::role_default(profile);
    let authorization = AuthorizationRevision {
        value: "exact-revision-parity",
        basis: AuthorizationBasis::AuthenticatedStoreScope,
    };
    let exact = SearchRequest {
        query: "task.create",
        purpose: None,
        task_id: Some("task-exact-parity"),
        exact_method: Some("task.create"),
        loaded_catalog_revision: None,
        max_results: 1,
    };
    let broad = SearchRequest {
        query: "task create",
        purpose: None,
        task_id: Some("task-exact-parity"),
        exact_method: None,
        loaded_catalog_revision: None,
        max_results: 5,
    };
    // Construction seam: each search rebuilds the fresh authorized view;
    // schema bytes inside the digests come from the immutable factory cache.
    reset_search_view_builds();
    let first =
        search_catalog(profile, &surface, exact, authorization, |_, _| true).expect("exact match");
    assert_eq!(first.matches.len(), 1);
    assert_eq!(first.matches[0].method, "task.create");
    let second = search_catalog(profile, &surface, exact, authorization, |_, _| true)
        .expect("repeated exact match");
    assert_eq!(search_view_builds(), 2);
    // Revision parity: exact and broad full views share one identity.
    assert_eq!(first.catalog_revision, second.catalog_revision);
    assert_eq!(first.surface_revision, second.surface_revision);
    let broad_result =
        search_catalog(profile, &surface, broad, authorization, |_, _| true).expect("broad view");
    assert_eq!(broad_result.catalog_revision, first.catalog_revision);
    // Valid loaded revision is honored by exact searches.
    let replay = search_catalog(
        profile,
        &surface,
        SearchRequest {
            loaded_catalog_revision: Some(&first.catalog_revision),
            ..exact
        },
        authorization,
        |_, _| true,
    )
    .expect("valid loaded revision is honored");
    assert_eq!(replay.catalog_revision, first.catalog_revision);
    assert_eq!(replay.matches.len(), 1);
    // Stale loaded revision fails closed for exact searches.
    let stale = search_catalog(
        profile,
        &surface,
        SearchRequest {
            loaded_catalog_revision: Some(
                "0000000000000000000000000000000000000000000000000000000000000000",
            ),
            ..exact
        },
        authorization,
        |_, _| true,
    )
    .expect_err("stale loaded revision must fail closed");
    assert_eq!(stale, CatalogError::StaleCatalogRevision);
}

#[test]
fn repeated_searches_reuse_factory_bytes_without_rebuild() {
    use swarm_contracts::mcp_catalog::{find_tool_spec, input_schema_bytes};

    let profile = McpToolProfile::Full;
    let surface = Surface::role_default(profile);
    let authorization = AuthorizationRevision {
        value: "factory-reuse-rev",
        basis: AuthorizationBasis::AuthenticatedStoreScope,
    };
    let (read_only, spec) = find_tool_spec("task.create").expect("task spec");
    let before = input_schema_bytes(spec, *read_only, false).expect("factory bytes");
    reset_search_view_builds();
    for _ in 0..6 {
        let exact = search_catalog(
            profile,
            &surface,
            SearchRequest {
                query: "task.create",
                purpose: None,
                task_id: Some("task-factory-reuse"),
                exact_method: Some("task.create"),
                loaded_catalog_revision: None,
                max_results: 1,
            },
            authorization,
            |_, _| true,
        )
        .expect("exact search");
        assert_eq!(exact.matches.len(), 1);
        let broad = search_catalog(
            profile,
            &surface,
            SearchRequest {
                query: "task",
                purpose: None,
                task_id: Some("task-factory-reuse"),
                exact_method: None,
                loaded_catalog_revision: None,
                max_results: 5,
            },
            authorization,
            |_, _| true,
        )
        .expect("broad search");
        assert!(!broad.matches.is_empty());
    }
    // Views rebuild fresh every call (no positive rights cache) ...
    assert_eq!(search_view_builds(), 12);
    // ... while the immutable factory bytes are never re-serialized.
    let after = input_schema_bytes(spec, *read_only, false).expect("factory bytes");
    assert!(std::sync::Arc::ptr_eq(&before, &after));
}

#[test]
fn warm_catalog_rechecks_live_membership_and_rejects_unknown_exact_method() {
    let profile = McpToolProfile::Full;
    let surface = Surface::role_default(profile);
    let request = SearchRequest {
        query: "task.create",
        purpose: None,
        task_id: Some("task-parity"),
        exact_method: Some("task.create"),
        loaded_catalog_revision: None,
        max_results: 1,
    };
    let authorization = AuthorizationRevision {
        value: "same-revision",
        basis: AuthorizationBasis::AuthenticatedStoreScope,
    };
    let allowed = search_catalog(profile, &surface, request, authorization, |_, _| true)
        .expect("cold discovery");
    assert_eq!(allowed.matches.len(), 1);
    for _ in 0..3 {
        let mut checked = false;
        let denied = search_catalog(
            profile,
            &surface,
            request,
            authorization,
            |method, context| {
                assert_eq!(context, Some("task-parity"));
                checked |= method == "task.create";
                method != "task.create"
            },
        )
        .expect("warm discovery checks current membership");
        assert!(checked);
        assert!(denied.matches.is_empty());
        assert_ne!(allowed.catalog_revision, denied.catalog_revision);
    }
    let unknown = search_catalog(
        profile,
        &surface,
        SearchRequest {
            query: "unknown.method",
            exact_method: Some("unknown.method"),
            ..request
        },
        authorization,
        |_, _| true,
    )
    .expect("unknown exact method is a miss");
    assert!(unknown.matches.is_empty());
    assert!(Surface::configured(profile, Some("unknown-profile"), &[], &[]).is_err());
}

#[test]
fn published_pr_description_effect_is_manual_and_manager_scoped() {
    let method = "github.pull_request.update_description";
    let entry = metadata_for(method).expect("PR write effect is discoverable");
    assert_eq!(entry.method, method);
    assert_eq!(entry.load_tier, super::LoadTier::ManualOnly);
    assert!(entry.audiences.contains(&ToolAudience::Manager));
    assert!(entry.audiences.contains(&ToolAudience::GmOperator));
}

#[test]
fn pr_description_readback_reconcile_is_manual_and_current_manager_scoped() {
    let method = "github.pull_request.reconcile_description";
    let entry = metadata_for(method).expect("PR readback reconciliation is discoverable");
    assert_eq!(entry.method, method);
    assert_eq!(entry.load_tier, super::LoadTier::ManualOnly);
    assert!(entry.audiences.contains(&ToolAudience::Manager));
    assert!(entry.audiences.contains(&ToolAudience::GmOperator));
}

#[test]
fn drifted_metadata_uses_exact_executable_input_fields() {
    let cases: &[(&str, &[&str], &[&str])] = &[
        (
            "attempt.bind_producer",
            &[
                "attempt_id",
                "assignment_id",
                "native_session_id",
                "native_run_id",
                "observation_id",
            ],
            &[
                "current Attempt manager authority",
                "already observed exact producer evidence",
            ],
        ),
        (
            "gm.handover",
            &["client_id"],
            &[
                "local Operator or exact current GM authority",
                "registered eligible target client",
            ],
        ),
    ];

    for (method, expected_fields, expected_context) in cases {
        let metadata = metadata_for(method).expect("method metadata exists");
        let (_, spec) = super::find_spec(method).expect("method ToolSpec exists");
        assert_eq!(metadata.required_input_fields, *expected_fields, "{method}");
        assert_eq!(metadata.required_context, *expected_context, "{method}");
        assert_eq!(metadata.required_input_fields, spec.required, "{method}");
    }
    super::validate_registry_metadata().expect("typed metadata matches executable schemas");
}

#[test]
fn scoped_filter_precedes_search_and_stales_existing_page_cursor() {
    let profile = McpToolProfile::Full;
    let surface = Surface::role_default(profile);
    let authorization = AuthorizationRevision {
        value: "grant-rev-1",
        basis: AuthorizationBasis::AuthenticatedStoreScope,
    };
    let page = list_tools_page(profile, &surface, None, authorization, |method, _| {
        method != "task.create"
    })
    .expect("catalog page should be bounded and serializable");
    let cursor = page
        .next_cursor
        .expect("legacy full surface has further pages");

    let hidden = search_catalog(
        profile,
        &surface,
        SearchRequest {
            query: "task.create",
            purpose: None,
            task_id: Some("task-hidden-by-scope"),
            exact_method: Some("task.create"),
            loaded_catalog_revision: None,
            max_results: 5,
        },
        authorization,
        |method, _| method != "task.create",
    )
    .expect("search must return a normal miss for an unauthorized method");
    assert!(hidden.matches.is_empty());
    assert!(
        !serde_json::to_string(&hidden)
            .expect("search result serializes")
            .contains("task.create")
    );

    let changed_grant = AuthorizationRevision {
        value: "grant-rev-2",
        basis: AuthorizationBasis::AuthenticatedStoreScope,
    };
    let stale = list_tools_page(
        profile,
        &surface,
        Some(&cursor),
        changed_grant,
        |method, _| method != "task.create",
    )
    .expect_err("cursor must be bound to the authorized grant revision");
    assert_eq!(stale, CatalogError::StaleCursor);
}

#[test]
fn manager_discovers_manual_github_effect_and_lists_it_only_after_explicit_loading() {
    let profile = McpToolProfile::Manager;
    let authorization = AuthorizationRevision {
        value: "manager-scope-rev-1",
        basis: AuthorizationBasis::AuthenticatedStoreScope,
    };
    let default_surface = Surface::role_default(profile);
    let discovery = search_catalog(
        profile,
        &default_surface,
        SearchRequest {
            query: "managed label",
            purpose: None,
            task_id: None,
            exact_method: Some("github.effect.managed_label"),
            loaded_catalog_revision: None,
            max_results: 5,
        },
        authorization,
        |_, _| true,
    )
    .expect("the authorized manager can discover the manual-only effect");
    assert_eq!(discovery.matches.len(), 1);
    assert_eq!(discovery.matches[0].method, "github.effect.managed_label");
    assert_eq!(
        discovery.matches[0].activation,
        super::ActivationDisposition::ReconnectSurfaceRequired
    );
    assert_eq!(
        discovery.matches[0].suggested_surface.exact_manual_methods,
        ["github.effect.managed_label"]
    );

    let manual_methods = vec!["github.effect.managed_label".to_owned()];
    let loaded_surface = Surface::configured(profile, None, &[], &manual_methods)
        .expect("the manager may explicitly load this manual-only tool");
    let mut cursor = None;
    let mut listed = false;
    loop {
        let page = list_tools_page(
            profile,
            &loaded_surface,
            cursor.as_deref(),
            authorization,
            |_, _| true,
        )
        .expect("the explicit manager surface is listable");
        let tools = serde_json::to_value(&page.tools).expect("tool page serializes");
        listed |= tools.as_array().is_some_and(|items| {
            items.iter().any(|tool| {
                tool["name"].as_str() == Some("github_effect_managed_label")
                    && tool["annotations"]["readOnlyHint"] != true
                    && tool["inputSchema"]["required"]
                        .as_array()
                        .is_some_and(|required| {
                            required.iter().any(|field| field == "client_request_id")
                        })
            })
        });
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    assert!(
        listed,
        "loaded manager surface lists the closed mutation schema"
    );
}

#[test]
fn catalog_v1_framing_and_intentional_drift_receipt() {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
    use swarm_contracts::mcp_catalog::{
        find_tool_spec, input_schema_bytes, mutation_requires_caller_request_id,
        output_schema_bytes, tool_name,
    };

    const AUTHORIZED_DOMAIN: &str = "eliot-mcp-authorized-catalog-v1";
    const SURFACE_DOMAIN: &str = "eliot-mcp-surface-view-v1";

    let drifted = metadata_for("attempt.bind_producer").expect("bind_producer metadata");
    let mut probe = *drifted;
    probe.required_input_fields = &[];
    let mut first = super::Sha256::new();
    super::digest_metadata(&mut first, drifted);
    let first = first.finalize();
    let mut second = super::Sha256::new();
    super::digest_metadata(&mut second, &probe);
    let second = second.finalize();
    assert_eq!(
        super::hex_digest(&first),
        super::hex_digest(&second),
        "required_input_fields must not affect V1 digest"
    );

    let (_, bind_spec) = find_tool_spec("attempt.bind_producer").expect("bind_producer spec");
    assert_eq!(
        drifted.required_input_fields, bind_spec.required,
        "bind_producer inputs match executable ToolSpec"
    );
    assert_eq!(
        drifted.purpose,
        "Associate an already observed native producer run with one exact Attempt."
    );
    assert_eq!(
        drifted.required_context,
        [
            "current Attempt manager authority",
            "already observed exact producer evidence"
        ]
    );
    assert_ne!(
        drifted.purpose,
        "Bind an attempt to an exact producer binding generation."
    );

    let handover = metadata_for("gm.handover").expect("handover metadata");
    let (_, handover_spec) = find_tool_spec("gm.handover").expect("handover spec");
    assert_eq!(handover.required_input_fields, handover_spec.required);
    assert_eq!(handover.required_input_fields, ["client_id"]);
    assert_eq!(
        handover.purpose,
        "Designate one registered eligible client as the current GM under the application epoch rules."
    );
    assert_ne!(
        handover.purpose,
        "Transfer the local manager lease through the guarded handover path."
    );

    let (_, send_spec) = find_tool_spec("coordination.send").expect("send spec");
    assert_eq!(
        send_spec.description,
        "Deliver a bounded non-null JSON body to one active Participant only when an exact current peer relation authorizes the separately revalidated source and target Task/revision/Attempt contexts; this is typed coordination, not raw mailbox access."
    );
    assert_ne!(
        send_spec.description,
        "Deliver a bounded non-null JSON body to one active Participant in the same current Task/Attempt scope; this is typed coordination, not raw mailbox access."
    );
    let send_meta = metadata_for("coordination.send").expect("send metadata");
    assert_eq!(
        send_meta.purpose,
        "Send one bounded typed coordination delivery to an active peer whose separately validated current Task/Attempt has an exact peer relation with the sender's current Task/Attempt."
    );
    assert_eq!(
        send_meta.required_context,
        [
            "recipient",
            "non-null JSON body <= 8 KiB",
            "authenticated current source and target contexts; exact current peer relation"
        ]
    );

    let profile = McpToolProfile::Observer;
    let method = "swarm.tools.search";
    let metadata = metadata_for(method).expect("stable metadata");
    let (read_only, spec) = find_tool_spec(method).expect("stable spec");
    assert_eq!(
        spec.description,
        "Search the caller's authorized MCP catalog by goal, purpose, or exact tool name. Returns metadata and a safe surface/reconnect recommendation only; it never executes a selected method."
    );
    let request_id = mutation_requires_caller_request_id(profile, spec.method, *read_only);
    assert!(!request_id, "read keeps one input variant");
    let input = input_schema_bytes(spec, *read_only, request_id).expect("stable input bytes");
    let output = output_schema_bytes(method).expect("stable output lookup");
    assert!(output.is_none(), "stable method has no output schema");

    let revision = "pr40-receipt-v1";
    let mut preimage = Vec::new();
    let mut frame = |value: &[u8]| {
        preimage.extend_from_slice(&(value.len() as u64).to_be_bytes());
        preimage.extend_from_slice(value);
    };
    frame(AUTHORIZED_DOMAIN.as_bytes());
    frame(super::profile_name(profile).as_bytes());
    frame(revision.as_bytes());
    frame(b"");
    frame(metadata.method.as_bytes());
    frame(metadata.group.as_str().as_bytes());
    frame(metadata.load_tier.as_str().as_bytes());
    frame(metadata.purpose.as_bytes());
    frame(metadata.when_to_use.as_bytes());
    frame(metadata.result_policy.as_bytes());
    for audience in metadata.audiences {
        frame(audience.as_str().as_bytes());
    }
    for term in metadata.search_terms {
        frame(term.as_bytes());
    }
    for context in metadata.required_context {
        frame(context.as_bytes());
    }
    frame(tool_name(method).as_bytes());
    frame(spec.description.as_bytes());
    frame(input.as_ref());
    frame(&[]);

    let expected = super::hex_digest(&super::Sha256::digest(&preimage));
    let actual = super::hex_digest(
        &super::digest_entries(profile, revision, "", &[metadata]).expect("one-entry digest"),
    );
    assert_eq!(expected, actual, "V1 preimage framing preserved");

    let mut surface = Surface::role_default(McpToolProfile::Full);
    surface.legacy_full = true;
    let authorization = AuthorizationRevision {
        value: "pr40-receipt-v1",
        basis: AuthorizationBasis::AuthenticatedStoreScope,
    };
    let page = list_tools_page(
        McpToolProfile::Full,
        &surface,
        None,
        authorization,
        |_, _| true,
    )
    .expect("first catalog page");
    let cursor = page.next_cursor.expect("legacy surface pages");
    let raw = URL_SAFE_NO_PAD.decode(&cursor).expect("cursor decodes");
    assert_eq!(raw.len(), 73, "cursor stays 73 bytes");
    assert_eq!(raw[0], 1, "cursor stays version 1");
    assert_eq!(SURFACE_DOMAIN, "eliot-mcp-surface-view-v1");
}
