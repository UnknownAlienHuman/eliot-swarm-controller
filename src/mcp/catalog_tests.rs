use super::{
    AuthorizationBasis, AuthorizationRevision, CatalogError, SearchRequest, Surface,
    list_tools_page, search_catalog,
};
use crate::config::McpToolProfile;

#[test]
fn published_pr_description_effect_is_manual_and_manager_scoped() {
    let method = "github.pull_request.update_description";
    let entry = super::metadata_for(method).expect("PR write effect is discoverable");
    assert_eq!(entry.method, method);
    assert_eq!(entry.load_tier, super::LoadTier::ManualOnly);
    assert!(entry.audiences.contains(&super::ToolAudience::Manager));
    assert!(entry.audiences.contains(&super::ToolAudience::GmOperator));
}

#[test]
fn pr_description_readback_reconcile_is_manual_and_current_manager_scoped() {
    let method = "github.pull_request.reconcile_description";
    let entry = super::metadata_for(method).expect("PR readback reconciliation is discoverable");
    assert_eq!(entry.method, method);
    assert_eq!(entry.load_tier, super::LoadTier::ManualOnly);
    assert!(entry.audiences.contains(&super::ToolAudience::Manager));
    assert!(entry.audiences.contains(&super::ToolAudience::GmOperator));
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
