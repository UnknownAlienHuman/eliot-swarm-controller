use super::{
    AuthorizationBasis, AuthorizationRevision, CatalogError, SearchRequest, Surface,
    list_tools_page, search_catalog,
};
use crate::config::McpToolProfile;

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
