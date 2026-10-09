from pathlib import Path


def replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{label}: expected one replacement, found {count}")
    return text.replace(old, new)


catalog_path = Path("crates/swarm-mcp/src/mcp/catalog.rs")
catalog = catalog_path.read_text(encoding="utf-8")

catalog = replace_once(
    catalog,
    '''    pub search_terms: &'static [&'static str],
    pub required_context: &'static [&'static str],
    pub result_policy: &'static str,
''',
    '''    pub search_terms: &'static [&'static str],
    /// Exact request fields required by the executable ToolSpec. Empty means
    /// this legacy metadata row has not yet opted into the checked contract.
    pub required_input_fields: &'static [&'static str],
    /// Semantic prerequisites which are not request-field names.
    pub required_context: &'static [&'static str],
    pub result_policy: &'static str,
''',
    "ToolMetadata fields",
)

catalog = replace_once(
    catalog,
    '''macro_rules! entry {
    ($method:literal, $group:ident, $aud:ident, $tier:ident, $purpose:literal, $when:literal, $terms:expr, $context:expr, $result:literal) => {
        ToolMetadata {
            method: $method,
            group: ToolGroup::$group,
            audiences: $aud,
            load_tier: LoadTier::$tier,
            purpose: $purpose,
            when_to_use: $when,
            search_terms: $terms,
            required_context: $context,
            result_policy: $result,
        }
    };
}
''',
    '''macro_rules! entry {
    ($method:literal, $group:ident, $aud:ident, $tier:ident, $purpose:literal, $when:literal, $terms:expr, $context:expr, $result:literal) => {
        ToolMetadata {
            method: $method,
            group: ToolGroup::$group,
            audiences: $aud,
            load_tier: LoadTier::$tier,
            purpose: $purpose,
            when_to_use: $when,
            search_terms: $terms,
            required_input_fields: &[],
            required_context: $context,
            result_policy: $result,
        }
    };
}

/// Opt one metadata row into an exact, machine-checked relationship with the
/// executable ToolSpec. The declared input fields must equal ToolSpec.required;
/// semantic prerequisites stay separate and cannot masquerade as arguments.
macro_rules! entry_with_inputs {
    ($method:literal, $group:ident, $aud:ident, $tier:ident, $purpose:literal, $when:literal, $terms:expr, $inputs:expr, $context:expr, $result:literal) => {
        ToolMetadata {
            method: $method,
            group: ToolGroup::$group,
            audiences: $aud,
            load_tier: LoadTier::$tier,
            purpose: $purpose,
            when_to_use: $when,
            search_terms: $terms,
            required_input_fields: $inputs,
            required_context: $context,
            result_policy: $result,
        }
    };
}
''',
    "entry macros",
)

catalog = replace_once(
    catalog,
    '''    entry!(
        "attempt.bind_producer",
        TaskManagement,
        MANAGER_AUDIENCES,
        ManualOnly,
        "Bind an attempt to an exact producer binding generation.",
        "Use only when explicitly binding the producer identity for an attempt.",
        &["attempt", "producer", "binding", "generation"],
        &[
            "attempt_id",
            "expected_revision",
            "binding_id",
            "binding_generation"
        ],
        "One revision-checked producer binding."
    ),
''',
    '''    entry_with_inputs!(
        "attempt.bind_producer",
        TaskManagement,
        MANAGER_AUDIENCES,
        ManualOnly,
        "Associate an already observed native producer run with one exact Attempt.",
        "Use only after retaining the exact assignment, native session/run and observation evidence; this starts no worker and consumes no result.",
        &[
            "attempt",
            "producer",
            "assignment",
            "native session",
            "native run",
            "observation"
        ],
        &[
            "attempt_id",
            "assignment_id",
            "native_session_id",
            "native_run_id",
            "observation_id"
        ],
        &[
            "current Attempt manager authority",
            "already observed exact producer evidence"
        ],
        "One evidence-bound producer association; no native effect or Task acceptance."
    ),
''',
    "attempt.bind_producer metadata",
)

catalog = replace_once(
    catalog,
    '''    entry!(
        "gm.handover",
        Administration,
        GM_AUDIENCES,
        ManualOnly,
        "Transfer the local manager lease through the guarded handover path.",
        "Use only for an explicit operator handover to a named eligible client.",
        &["manager", "handover", "lease", "operator"],
        &["target_client_id", "expected_revision"],
        "One guarded manager handover."
    ),
''',
    '''    entry_with_inputs!(
        "gm.handover",
        Administration,
        GM_AUDIENCES,
        ManualOnly,
        "Designate one registered eligible client as the current GM under the application epoch rules.",
        "Use only for an explicit guarded handover; optional binding identity is part of the request but not required.",
        &["manager", "GM", "handover", "designation", "epoch", "operator"],
        &["client_id"],
        &[
            "local Operator or exact current GM authority",
            "registered eligible target client"
        ],
        "One guarded GM designation with retained epoch identity."
    ),
''',
    "gm.handover metadata",
)

catalog = replace_once(
    catalog,
    '''    for metadata in TOOL_METADATA {
        if !seen.insert(metadata.method) || find_spec(metadata.method).is_none() {
            return Err(CatalogError::IncompleteRegistry);
        }
    }
''',
    '''    for metadata in TOOL_METADATA {
        let Some((_, spec)) = find_spec(metadata.method) else {
            return Err(CatalogError::IncompleteRegistry);
        };
        if !seen.insert(metadata.method) || !metadata_input_contract_matches(metadata, spec) {
            return Err(CatalogError::IncompleteRegistry);
        }
    }
''',
    "registry validation loop",
)

catalog = replace_once(
    catalog,
    '''pub fn metadata_for(method: &str) -> Option<&'static ToolMetadata> {
''',
    '''fn metadata_input_contract_matches(metadata: &ToolMetadata, spec: &ToolSpec) -> bool {
    if metadata.required_input_fields.is_empty() {
        return true;
    }
    let declared: BTreeSet<_> = metadata.required_input_fields.iter().copied().collect();
    let required: BTreeSet<_> = spec.required.iter().copied().collect();
    declared.len() == metadata.required_input_fields.len()
        && declared == required
        && declared
            .iter()
            .all(|field| spec.fields.iter().any(|candidate| candidate.name == *field))
}

pub fn metadata_for(method: &str) -> Option<&'static ToolMetadata> {
''',
    "input contract helper",
)

catalog = replace_once(
    catalog,
    '''    pub search_terms: &'static [&'static str],
    pub required_context: &'static [&'static str],
    pub result_policy: &'static str,
''',
    '''    pub search_terms: &'static [&'static str],
    pub required_input_fields: &'static [&'static str],
    pub required_context: &'static [&'static str],
    pub result_policy: &'static str,
''',
    "CatalogMatch fields",
)

catalog = replace_once(
    catalog,
    '''                search_terms: metadata.search_terms,
                required_context: metadata.required_context,
                result_policy: metadata.result_policy,
''',
    '''                search_terms: metadata.search_terms,
                required_input_fields: metadata.required_input_fields,
                required_context: metadata.required_context,
                result_policy: metadata.result_policy,
''',
    "CatalogMatch projection",
)

catalog = replace_once(
    catalog,
    '''    for term in metadata.search_terms {
        update_field(hasher, term.as_bytes());
    }
    for context in metadata.required_context {
''',
    '''    for term in metadata.search_terms {
        update_field(hasher, term.as_bytes());
    }
    for field in metadata.required_input_fields {
        update_field(hasher, field.as_bytes());
    }
    for context in metadata.required_context {
''',
    "catalog digest",
)

catalog = replace_once(
    catalog,
    '''        metadata.search_terms.join(" "),
        metadata.required_context.join(" "),
        metadata.result_policy
''',
    '''        metadata.search_terms.join(" "),
        metadata.required_input_fields.join(" "),
        metadata.required_context.join(" "),
        metadata.result_policy
''',
    "search text",
)

catalog_path.write_text(catalog, encoding="utf-8")

tests_path = Path("crates/swarm-mcp/src/mcp/catalog_tests.rs")
tests = tests_path.read_text(encoding="utf-8")
test_insert = '''
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
        let metadata = super::metadata_for(method).expect("method metadata exists");
        let (_, spec) = super::find_spec(method).expect("method ToolSpec exists");
        assert_eq!(metadata.required_input_fields, *expected_fields, "{method}");
        assert_eq!(metadata.required_context, *expected_context, "{method}");
        assert_eq!(metadata.required_input_fields, spec.required, "{method}");
        assert!(super::metadata_input_contract_matches(metadata, spec));
    }
    super::validate_registry_metadata().expect("typed metadata matches executable schemas");
}

'''
tests = replace_once(
    tests,
    '''#[test]
fn scoped_filter_precedes_search_and_stales_existing_page_cursor() {
''',
    test_insert + '''#[test]
fn scoped_filter_precedes_search_and_stales_existing_page_cursor() {
''',
    "catalog regression test",
)
tests_path.write_text(tests, encoding="utf-8")
