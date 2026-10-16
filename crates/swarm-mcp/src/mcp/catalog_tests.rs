use super::{
    AuthorizationBasis, AuthorizationRevision, CatalogError, SearchRequest, Surface,
    list_tools_page, search_catalog,
};
use crate::config::McpToolProfile;
use sha2::Digest;
use swarm_contracts::mcp_catalog::{ToolAudience, exposes_method, metadata_for};

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
fn descriptor_install_is_limited_to_the_local_operator_profile() {
    const METHOD: &str = "module.descriptor.install";

    let metadata = metadata_for(METHOD).expect("descriptor install has catalog metadata");
    assert_eq!(metadata.audiences, &[ToolAudience::FullCompatibility]);
    assert!(exposes_method(McpToolProfile::Full, METHOD));
    for profile in [
        McpToolProfile::Observer,
        McpToolProfile::Reviewer,
        McpToolProfile::Participant,
        McpToolProfile::AssignedReviewer,
        McpToolProfile::Manager,
        McpToolProfile::Gm,
    ] {
        assert!(
            !exposes_method(profile, METHOD),
            "unexpected exposure: {profile:?}"
        );
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
    let first =
        search_catalog(profile, &surface, exact, authorization, |_, _| true).expect("exact match");
    assert_eq!(first.matches.len(), 1);
    assert_eq!(first.matches[0].method, "task.create");
    let second = search_catalog(profile, &surface, exact, authorization, |_, _| true)
        .expect("repeated exact match");
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
fn catalog_v1_historical_unchanged_rows_and_current_schema_parity() {
    use swarm_contracts::mcp_catalog::{
        TOOL_METADATA, input_schema_bytes, mutation_requires_caller_request_id,
        output_schema_bytes, validate_registry_metadata,
    };

    const PROFILE_CASES: [(McpToolProfile, &str); 7] = [
        (McpToolProfile::Observer, "observer"),
        (McpToolProfile::Reviewer, "reviewer"),
        (McpToolProfile::Participant, "participant"),
        (McpToolProfile::AssignedReviewer, "assigned_reviewer"),
        (McpToolProfile::Manager, "manager"),
        (McpToolProfile::Gm, "gm"),
        (McpToolProfile::Full, "full"),
    ];
    const PROFILE_NAMES: [&str; 7] = [
        "observer",
        "reviewer",
        "participant",
        "assigned_reviewer",
        "manager",
        "gm",
        "full",
    ];
    const AUDIENCES: [&str; 7] = [
        "observer",
        "legacy-reviewer",
        "assigned-reviewer",
        "participant",
        "manager",
        "gm-operator",
        "full-compatibility",
    ];
    const DELTA_METHODS: &[&str] = &[
        "agent.family",
        "agent.usage",
        "check.acknowledge_held",
        "coordination.contract.ratify",
        "coordination.contract.reject",
        "coordination.contract_card.get",
        "coordination.contract_card.list",
        "coordination.inbox",
        "coordination.peer.find",
        "coordination.send",
        "coordination.thread.list",
        "coordination.watch.list",
        "coordination.work_card.list",
        "module.descriptor.install",
        "report.delta",
        "review.submit",
        "runtime.catalog",
        "swarm.tools.search",
        "task.accept",
        "task.create",
        "task.request_changes",
        "task.revise",
        "task.submit",
    ];

    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/pr40_catalog_v1_parity.json"))
            .expect("retained pre-extraction Catalog V1 fixture parses");
    assert_eq!(
        fixture["historical_source"]["commit"],
        "88264832b7cde56b917728830666f27b29fc515e"
    );
    assert_eq!(
        fixture["historical_source"]["blobs"]["crates/swarm-mcp/src/mcp/mod.rs"],
        "128fe97bcf2a366a119ce39f52a0726f5c0253e8"
    );
    assert_eq!(
        fixture["historical_source"]["blobs"]["crates/swarm-mcp/src/mcp/catalog.rs"],
        "07842427ee029f54437e1e00417bdf97eba3d6f5"
    );
    assert_eq!(
        fixture["current_source_baseline"],
        "6ec3bec9a551a6680409ff7cb33d7fde042a581b"
    );
    assert_eq!(
        fixture["v1"]["authorized_catalog_domain"],
        "eliot-mcp-authorized-catalog-v1"
    );
    assert_eq!(fixture["v1"]["surface_domain"], "eliot-mcp-surface-view-v1");
    assert_eq!(
        fixture["v1"]["metadata_sort"],
        serde_json::json!(["group", "load_tier", "method"])
    );
    assert_eq!(
        fixture["v1"]["metadata_order"],
        serde_json::json!([
            "method",
            "group",
            "load_tier",
            "purpose",
            "when_to_use",
            "result_policy",
            "audiences[]",
            "search_terms[]",
            "required_context[]"
        ])
    );
    assert_eq!(fixture["v1"]["profiles"], serde_json::json!(PROFILE_NAMES));
    assert_eq!(fixture["v1"]["audiences"], serde_json::json!(AUDIENCES));

    let deltas = fixture["source_deltas"]
        .as_array()
        .expect("source delta rows");
    let mut actual_delta_methods: Vec<_> = deltas
        .iter()
        .map(|row| row["method"].as_str().expect("delta method"))
        .collect();
    actual_delta_methods.sort_unstable();
    assert_eq!(actual_delta_methods, DELTA_METHODS);
    assert_eq!(
        deltas
            .iter()
            .filter(|row| row["state"] == "changed")
            .count(),
        17,
        "only the retained changed rows are excluded from historical parity"
    );
    assert_eq!(
        deltas.iter().filter(|row| row["state"] == "added").count(),
        6,
        "only the retained additions are excluded from historical parity"
    );

    let send_delta = deltas
        .iter()
        .find(|row| row["method"] == "coordination.send")
        .expect("the separately reviewed #99 wording delta is explicit");
    let (_, send_spec) = super::find_spec("coordination.send").expect("send ToolSpec");
    assert_eq!(
        send_spec.description,
        send_delta["current_tool_description"]
            .as_str()
            .expect("current send description")
    );
    assert_eq!(
        send_delta["historical_tool_description"],
        "Deliver a bounded non-null JSON body to one active Participant in the same current Task/Attempt scope; this is typed coordination, not raw mailbox access."
    );
    assert_eq!(
        send_delta["wire_fields"],
        serde_json::json!(["description"])
    );
    assert!(
        send_delta["source_note"]
            .as_str()
            .is_some_and(|note| note.contains("#99"))
    );

    // Validation-only input metadata is deliberately outside the Catalog V1 preimage.
    let bind = metadata_for("attempt.bind_producer").expect("bind metadata");
    let mut without_validation_fields = *bind;
    without_validation_fields.required_input_fields = &[];
    let mut original_metadata = super::Sha256::new();
    super::digest_metadata(&mut original_metadata, bind);
    let mut validation_fields_removed = super::Sha256::new();
    super::digest_metadata(&mut validation_fields_removed, &without_validation_fields);
    assert_eq!(
        super::hex_digest(&original_metadata.finalize()),
        super::hex_digest(&validation_fields_removed.finalize()),
        "required_input_fields remain outside Catalog V1"
    );

    validate_registry_metadata().expect("runtime metadata validates against executable ToolSpecs");
    let authorization_revision = fixture["v1"]["authorization_revision"]
        .as_str()
        .expect("fixture authorization revision");
    let authorization_context = fixture["v1"]["authorization_context"]
        .as_str()
        .expect("fixture authorization context");
    let authorization = AuthorizationRevision {
        value: authorization_revision,
        basis: AuthorizationBasis::AuthenticatedStoreScope,
    };

    for (profile, profile_name) in PROFILE_CASES {
        let profile_fixture = &fixture["profiles"][profile_name];
        let mut surface = Surface::role_default(profile);
        surface.legacy_full = true;

        let mut authorization_checks = 0usize;
        let view = super::surface_view(profile, &surface, authorization, None, &mut |_, _| {
            authorization_checks += 1;
            true
        })
        .expect("current full Catalog V1 surface");
        let current_catalog_digest = super::hex_digest(&view.authorized.catalog_digest);
        let current_surface_digest = super::hex_digest(&view.surface_digest);
        assert_eq!(
            current_catalog_digest,
            profile_fixture["current"]["catalog_v1"]
                .as_str()
                .expect("current Catalog V1 digest"),
            "current catalog digest for {profile_name}"
        );
        assert_eq!(
            current_surface_digest,
            profile_fixture["current"]["surface_v1"]
                .as_str()
                .expect("current surface digest"),
            "current surface digest for {profile_name}"
        );
        assert_eq!(
            view.authorized.entries.len(),
            profile_fixture["current"]["method_count"]
                .as_u64()
                .expect("current method count") as usize
        );
        assert_eq!(view.visible.len(), view.authorized.entries.len());
        assert_eq!(authorization_checks, view.authorized.entries.len());

        let mut unchanged_entries: Vec<_> = TOOL_METADATA
            .iter()
            .filter(|metadata| super::profiles::exposes_method(profile, metadata.method))
            .filter(|metadata| !DELTA_METHODS.contains(&metadata.method))
            .collect();
        unchanged_entries.sort_by(|left, right| super::metadata_order(left, right));
        let historical_unchanged = &profile_fixture["unchanged"];
        assert_eq!(
            unchanged_entries.len(),
            historical_unchanged["row_count"]
                .as_u64()
                .expect("historical unchanged row count") as usize
        );
        let unchanged_catalog_digest = super::digest_entries(
            profile,
            authorization_revision,
            authorization_context,
            &unchanged_entries,
        )
        .expect("unchanged row Catalog V1 digest");
        assert_eq!(
            super::hex_digest(&unchanged_catalog_digest),
            historical_unchanged["catalog_v1"]
                .as_str()
                .expect("historical unchanged Catalog V1 digest"),
            "unchanged Catalog V1 rows for {profile_name}"
        );
        let unchanged_surface_digest = super::digest_surface(
            &surface,
            unchanged_catalog_digest,
            profile,
            &unchanged_entries,
        )
        .expect("unchanged row surface digest");
        assert_eq!(
            super::hex_digest(&unchanged_surface_digest),
            historical_unchanged["surface_v1"]
                .as_str()
                .expect("historical unchanged surface digest"),
            "unchanged surface rows for {profile_name}"
        );

        let mut schema_bytes_per_digest = 0usize;
        for metadata in &view.authorized.entries {
            let (read_only, spec) = super::find_spec(metadata.method).expect("authorized ToolSpec");
            let request_id_required =
                mutation_requires_caller_request_id(profile, spec.method, *read_only);
            schema_bytes_per_digest += input_schema_bytes(spec, *read_only, request_id_required)
                .expect("cached input bytes")
                .len();
            if let Some(output) = output_schema_bytes(spec.method).expect("cached output bytes") {
                schema_bytes_per_digest += output.len();
            }
        }

        let mut cursor = None;
        let mut listed_tools = Vec::new();
        let mut list_pages = 0usize;
        let mut list_authorization_checks = 0usize;
        loop {
            list_pages += 1;
            let page = list_tools_page(
                profile,
                &surface,
                cursor.as_deref(),
                authorization,
                |_, _| {
                    list_authorization_checks += 1;
                    true
                },
            )
            .expect("current Catalog V1 page");
            listed_tools.extend(page.tools);
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(listed_tools.len(), view.visible.len());
        assert_eq!(
            list_authorization_checks,
            list_pages * view.authorized.entries.len(),
            "list authorization remains live for every page"
        );
        let serialized_tool_bytes = serde_json::to_vec(&listed_tools)
            .expect("Catalog V1 DTO bytes")
            .len();
        assert_eq!(
            serialized_tool_bytes,
            profile_fixture["current"]["serialized_tool_bytes"]
                .as_u64()
                .expect("current serialized tool byte count") as usize
        );

        println!(
            "PR40_CATALOG_COST profile={} rows={} schema_bytes_per_catalog_digest={} schema_bytes_per_surface_digest={} pages={} serialized_tool_bytes={} authorization_checks={} list_authorization_checks={} target_dispatches=0",
            profile_name,
            view.authorized.entries.len(),
            schema_bytes_per_digest,
            schema_bytes_per_digest,
            list_pages,
            serialized_tool_bytes,
            authorization_checks,
            list_authorization_checks,
        );

        for delta in deltas {
            let method = delta["method"].as_str().expect("delta method");
            let expected_row = delta["current_row_digests"].get(profile_name);
            let visible = super::profiles::exposes_method(profile, method);
            assert_eq!(
                expected_row.is_some(),
                visible,
                "explicit delta membership for {method} {profile_name}"
            );
            if let Some(expected_row) = expected_row {
                let metadata = metadata_for(method).expect("delta metadata");
                let row_digest = super::digest_entries(
                    profile,
                    authorization_revision,
                    authorization_context,
                    &[metadata],
                )
                .expect("current delta row digest");
                let current_hash = super::hex_digest(&row_digest);
                assert_eq!(
                    current_hash,
                    expected_row.as_str().expect("current row digest"),
                    "current Catalog V1 delta row {method} {profile_name}"
                );
                if delta["state"] == "changed" {
                    let historical_hash = delta["historical_row_digests"][profile_name]
                        .as_str()
                        .expect("historical changed-row digest");
                    assert_ne!(current_hash, historical_hash, "source delta {method}");
                } else {
                    assert_eq!(delta["state"], "added", "new source row {method}");
                    assert!(
                        delta["historical_row_digests"]
                            .as_object()
                            .unwrap()
                            .is_empty()
                    );
                }
            }
        }
    }
}
