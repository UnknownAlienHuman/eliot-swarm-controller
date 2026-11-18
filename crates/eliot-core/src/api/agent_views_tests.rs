use super::super::agent_requests::*;
use super::*;
use crate::ProcessState;
use serde_json::json;

fn cursor() -> ProcessPageCursor {
    ProcessPageCursor {
        agent: "a1".to_owned(),
        generation: 1,
        scope: "workspace".to_owned(),
        identity: ProcessIdentity {
            host_boot_id: "boot".to_owned(),
            pid: 7,
            start_time: 9,
        },
    }
}

fn process() -> ProcessRecord {
    ProcessRecord {
        identity: cursor().identity,
        owner: "workspace:a1#1".to_owned(),
        role: "setup".to_owned(),
        state: ProcessState::Exited,
        exit_code: None,
        updated_at: 0,
    }
}

fn observation() -> ObservationView {
    serde_json::from_value(json!({"available":false,"error_code":"unavailable"})).unwrap()
}

fn unknown(result: Result<()>) {
    let error = result.unwrap_err();
    assert_eq!(error.code, "unknown");
    assert_eq!(error.message.as_ref(), "Agent view response is invalid");
    assert!(error.diagnostics.is_empty());
}

#[test]
fn project_registration_readback_preserves_explicit_empty_allowsets() {
    let view = ProjectRegistrationView {
        id: "project-a".to_owned(),
        checkout: r"C:\work\project-a".to_owned(),
        repo_common_dir: r"C:\work\project-a\.git".to_owned(),
        profiles: Vec::new(),
        actions: Vec::new(),
        interpreters: BTreeMap::new(),
        run_env: Vec::new(),
        git: None,
        revision: 1,
        origin: None,
        checkouts: Vec::new(),
    };
    view.validate().unwrap();
    let wire = serde_json::to_value(&view).unwrap();
    // No origin read: the field is absent, not null.
    assert!(wire.get("origin").is_none());
    assert_eq!(wire["profiles"], json!([]));
    assert_eq!(wire["actions"], json!([]));
    assert_eq!(wire["interpreters"], json!({}));
    assert_eq!(wire["run_env"], json!([]));
    assert_eq!(wire["git"], Value::Null);
    assert_eq!(
        serde_json::from_value::<ProjectRegistrationView>(wire.clone()).unwrap(),
        view
    );
    let mut old_wire = wire.clone();
    old_wire.as_object_mut().unwrap().remove("git");
    assert_eq!(
        serde_json::from_value::<ProjectRegistrationView>(old_wire)
            .unwrap()
            .git,
        None
    );
    let mut unknown = wire;
    unknown["future"] = json!(true);
    assert!(serde_json::from_value::<ProjectRegistrationView>(unknown).is_err());

    let mut invalid = view.clone();
    invalid.profiles = vec!["fake".to_owned(), "fake".to_owned()];
    assert!(invalid.validate().is_err());
    invalid = view.clone();
    invalid.actions = vec!["stop".to_owned(), " ".to_owned()];
    assert!(invalid.validate().is_err());

    let mut named = view;
    named.origin = Some("atlas-web".to_owned());
    named.validate().unwrap();
    for bad in ["", "org/atlas", "user@host", "a b", "..", "x:y"] {
        named.origin = Some(bad.to_owned());
        assert!(named.validate().is_err(), "{bad:?}");
    }
}

#[test]
fn origin_urls_read_as_their_repository_name_without_credentials() {
    for (url, name) in [
        ("https://github.com/acme/atlas-web.git", Some("atlas-web")),
        (
            "https://user:secret@example.com/acme/atlas.git/",
            Some("atlas"),
        ),
        ("git@github.com:acme/atlas.git", Some("atlas")),
        ("ssh://git@host:2222/srv/atlas", Some("atlas")),
        ("file:///srv/git/tools.git", Some("tools")),
        ("/srv/git/tools.git\n", Some("tools")),
        (r"C:\repos\atlas.git", Some("atlas")),
        ("https://host/acme/atlas.git?token=secret", Some("atlas")),
        ("atlas", Some("atlas")),
        ("https://host/", None),
        ("", None),
        ("   ", None),
        ("https://host/acme/.git", None),
    ] {
        assert_eq!(repository_name_from_url(url).as_deref(), name, "{url:?}");
    }
}

#[test]
fn hostile_origin_urls_never_yield_userinfo_or_authority() {
    for remote in [
        "https://alice:prefix?fragment@host",
        "https://alice:prefix/fragment@host",
        "https://alice/prefix#fragment@host/repo",
        "https://alice:prefix#fragment@host/repo",
        "https://alice:secret@host?query=/repo",
        "https://host:8443",
        "https://host:invalid/repo",
        "https:///repo",
        "https:/repo",
        "ssh://git@host",
        "git@host:",
        "ext::git upload-pack repo",
        "https://host/team/%40masked",
        "https://host/team/repo\nhttps://other/another",
    ] {
        assert_eq!(repository_name_from_url(remote), None, "{remote:?}");
    }
    assert_eq!(
        repository_name_from_url("git@[::1]:/srv/tools.git").as_deref(),
        Some("tools")
    );
    assert_eq!(
        repository_name_from_url("../local/tools.git").as_deref(),
        Some("tools")
    );
}

#[test]
fn project_git_view_roundtrips_only_approved_literal_endpoints_and_helper() {
    let view = ProjectGitView {
        fetch_remotes: BTreeMap::from([(
            "origin".to_owned(),
            "https://git.example.test/team/repo.git".to_owned(),
        )]),
        credential_helper: Some(GitCredentialHelper::Manager),
    };
    view.validate().unwrap();
    let wire = serde_json::to_value(&view).unwrap();
    assert_eq!(
        wire["fetch_remotes"]["origin"],
        "https://git.example.test/team/repo.git"
    );
    assert_eq!(wire["credential_helper"], "manager");
    assert_eq!(
        serde_json::from_value::<ProjectGitView>(wire).unwrap(),
        view
    );

    for (remote, endpoint) in [
        ("--upload-pack=evil", "https://git.example.test/repo.git"),
        ("origin", " "),
        ("origin", "https://git.example.test/\nrepo.git"),
    ] {
        let invalid = ProjectGitView {
            fetch_remotes: BTreeMap::from([(remote.to_owned(), endpoint.to_owned())]),
            credential_helper: None,
        };
        assert_eq!(invalid.validate().unwrap_err().code, "invalid");
    }
}

#[test]
fn view_defaults_preserve_null_as_absent_and_omit_optional_fields() {
    let request: AgentProcessesRequest = serde_json::from_value(json!({
        "id":" a1 ","generation":null,"scope":null,"after":null
    }))
    .unwrap();
    assert_eq!(request, AgentProcessesRequest::new(" a1 "));
    assert_eq!(
        serde_json::to_value(&request).unwrap(),
        json!({"id":" a1 ","limit":100})
    );
    assert_eq!(request.effective_scope(), "agent");
    request.validate().unwrap();
    assert_eq!(
        serde_json::from_value::<AgentListRequest>(json!({"after":null})).unwrap(),
        AgentListRequest::default()
    );
    assert_eq!(
        serde_json::from_value::<AgentStatsRequest>(json!({"id":"a1","generation":null})).unwrap(),
        AgentStatsRequest::new("a1")
    );
    assert_eq!(
        serde_json::from_value::<AgentGitRequest>(json!({"id":"a1","generation":null})).unwrap(),
        AgentGitRequest::new("a1")
    );
    assert_eq!(
        serde_json::from_value::<AgentEditsRequest>(
            json!({"id":"a1","generation":null,"after":null})
        )
        .unwrap(),
        AgentEditsRequest::new("a1")
    );
    assert!(
        serde_json::from_value::<DaemonStatusRequest>(json!({}))
            .unwrap()
            .validate()
            .is_ok()
    );
}

#[test]
fn required_ids_and_unknown_request_fields_are_strict() {
    for request in [
        json!({}),
        json!({"id":null}),
        json!({"id":false}),
        json!({"id":1}),
        json!({"id":" \t "}),
        json!({"id":"a1","extra":0}),
    ] {
        assert!(serde_json::from_value::<AgentGetRequest>(request.clone()).is_err());
        assert!(serde_json::from_value::<AgentProcessesRequest>(request.clone()).is_err());
        assert!(serde_json::from_value::<AgentStatsRequest>(request.clone()).is_err());
        assert!(serde_json::from_value::<AgentGitRequest>(request.clone()).is_err());
        assert!(serde_json::from_value::<AgentEditsRequest>(request).is_err());
    }
    assert!(serde_json::from_value::<AgentListRequest>(json!({"extra":0})).is_err());
    assert!(serde_json::from_value::<DaemonStatusRequest>(json!({"extra":0})).is_err());
    assert_eq!(
        serde_json::from_value::<AgentGetRequest>(json!({"id":" raw id "}))
            .unwrap()
            .id,
        " raw id "
    );
}

#[test]
fn supplied_limits_observations_and_optional_types_do_not_fall_back() {
    for bad in [
        Value::Null,
        json!(true),
        json!("1"),
        json!(1.0),
        json!(-1),
        json!(0),
        json!(1001),
        json!(u64::MAX),
    ] {
        assert!(serde_json::from_value::<AgentListRequest>(json!({"limit":bad})).is_err());
        assert!(
            serde_json::from_value::<AgentProcessesRequest>(json!({"id":"a1","limit":bad}))
                .is_err()
        );
        assert!(
            serde_json::from_value::<AgentEditsRequest>(json!({"id":"a1","limit":bad})).is_err()
        );
    }
    for bad in [Value::Null, json!(0), json!("false"), json!([])] {
        assert!(serde_json::from_value::<AgentListRequest>(json!({"observations":bad})).is_err());
    }
    for bad in [
        json!(-1),
        json!(true),
        json!("1"),
        json!(1.0),
        json!(i64::MAX as u64 + 1),
    ] {
        assert!(
            serde_json::from_value::<AgentProcessesRequest>(json!({"id":"a1","generation":bad}))
                .is_err()
        );
        assert!(
            serde_json::from_value::<AgentStatsRequest>(json!({"id":"a1","generation":bad}))
                .is_err()
        );
        assert!(
            serde_json::from_value::<AgentGitRequest>(json!({"id":"a1","generation":bad})).is_err()
        );
        assert!(
            serde_json::from_value::<AgentEditsRequest>(json!({"id":"a1","generation":bad}))
                .is_err()
        );
    }
    for bad in [json!(false), json!(1), json!([]), json!(""), json!(" ")] {
        assert!(serde_json::from_value::<AgentListRequest>(json!({"after":bad})).is_err());
        assert!(
            serde_json::from_value::<AgentProcessesRequest>(json!({"id":"a1","after":bad}))
                .is_err()
        );
    }
    for bad in [
        json!(false),
        json!(1),
        json!([]),
        json!("other"),
        json!(" workspace "),
    ] {
        assert!(
            serde_json::from_value::<AgentProcessesRequest>(json!({"id":"a1","scope":bad}))
                .is_err()
        );
    }
    for limit in [1, 1000] {
        let request: AgentListRequest = serde_json::from_value(json!({"limit":limit})).unwrap();
        request.validate().unwrap();
    }
}

#[test]
fn edit_cursor_is_opaque_without_trim_or_path_normalization() {
    for path in [" ", "\n", r"\\?\C:\work\note.txt", "é"] {
        let request: AgentEditsRequest =
            serde_json::from_value(json!({"id":"a1","after":path})).unwrap();
        request.validate().unwrap();
        assert_eq!(request.after.as_deref(), Some(path));
    }
    let mut request = AgentEditsRequest::new("a1");
    request.after = Some("é".repeat(EDIT_CURSOR_MAX_BYTES / 2));
    request.validate().unwrap();
    request.after.as_mut().unwrap().push('x');
    assert!(request.validate().is_err());
    for bad in [json!(""), json!("x\0y"), json!(false), json!(1), json!([])] {
        assert!(
            serde_json::from_value::<AgentEditsRequest>(json!({"id":"a1","after":bad})).is_err()
        );
    }
}

#[test]
fn process_cursor_requires_objects_and_strict_fields_at_both_levels() {
    let valid = cursor();
    assert_eq!(
        ProcessPageCursor::decode(&valid.encode().unwrap()).unwrap(),
        valid
    );
    let encoded = serde_json::to_value(&valid).unwrap();
    for value in [
        json!(["a1",1,"workspace",{"host_boot_id":"boot","pid":7,"start_time":9}]),
        json!(null),
        json!(true),
    ] {
        assert!(ProcessPageCursor::decode(&value.to_string()).is_err());
    }
    for field in ["agent", "generation", "scope", "identity"] {
        let mut missing = encoded.clone();
        missing.as_object_mut().unwrap().remove(field);
        assert!(ProcessPageCursor::decode(&missing.to_string()).is_err());
    }
    let mut extra = encoded.clone();
    extra["unknown"] = json!(1);
    assert!(ProcessPageCursor::decode(&extra.to_string()).is_err());
    let mut extra = encoded.clone();
    extra["identity"]["unknown"] = json!(1);
    assert!(ProcessPageCursor::decode(&extra.to_string()).is_err());
    let mut array = encoded;
    array["identity"] = json!(["boot", 7, 9]);
    assert!(ProcessPageCursor::decode(&array.to_string()).is_err());
}

#[test]
fn process_cursor_identity_bounds_and_post_resolution_binding_are_separate() {
    for identity in [
        ProcessIdentity {
            pid: 0,
            ..cursor().identity
        },
        ProcessIdentity {
            start_time: 0,
            ..cursor().identity
        },
        ProcessIdentity {
            host_boot_id: String::new(),
            ..cursor().identity
        },
        ProcessIdentity {
            host_boot_id: "b".repeat(513),
            ..cursor().identity
        },
        ProcessIdentity {
            host_boot_id: "b\n".to_owned(),
            ..cursor().identity
        },
    ] {
        let invalid = ProcessPageCursor {
            identity,
            ..cursor()
        };
        assert!(ProcessPageCursor::decode(&serde_json::to_string(&invalid).unwrap()).is_err());
    }
    assert!(ProcessPageCursor::decode(&" ".repeat(PROCESS_CURSOR_MAX_BYTES + 1)).is_err());
    let mut huge = cursor();
    huge.agent = "a".repeat(PROCESS_CURSOR_MAX_BYTES);
    assert_eq!(huge.encode().unwrap_err().code, "frame_too_large");
    let mut request = AgentProcessesRequest::new("different");
    request.generation = Some(999);
    request.after = Some(cursor().encode().unwrap());
    request.validate().unwrap(); // Store/authorization error precedence is preserved.
    let cursor = request.cursor().unwrap().unwrap();
    assert!(cursor.validate_binding("different", 999, "agent").is_err());
    cursor.validate_binding("a1", 1, "workspace").unwrap();
    request.scope = Some("workspace".to_owned());
    request.generation = Some(i64::MAX);
    request.validate().unwrap(); // No Core rejection of a future workspace generation.
}

#[test]
fn flattened_get_and_list_roundtrip_and_reject_nested_arrays() {
    let agent = Agent::new("a1", "project", "fake", "fake", 0);
    let reply = AgentGetResponse {
        agent: agent.clone(),
        observation: observation(),
        source_states: vec![],
        session_usage: None,
        config: None,
        heavy: None,
        session_growth: None,
    };
    let wire = serde_json::to_value(&reply).unwrap();
    assert_eq!(wire["id"], "a1");
    assert!(wire.get("agent").is_none());
    assert_eq!(
        serde_json::from_value::<AgentGetResponse>(wire.clone()).unwrap(),
        reply
    );
    AgentGetRequest::new("a1")
        .validate_response(&reply)
        .unwrap();
    unknown(AgentGetRequest::new("a2").validate_response(&reply));
    for field in ["observation", "source_states"] {
        let mut missing = wire.clone();
        missing.as_object_mut().unwrap().remove(field);
        assert!(serde_json::from_value::<AgentGetResponse>(missing).is_err());
    }
    let mut malformed = wire.clone();
    malformed["observation"] = json!([false, "unavailable"]);
    assert!(serde_json::from_value::<AgentGetResponse>(malformed).is_err());
    let mut malformed = wire.clone();
    malformed["source_states"] = json!([[]]);
    assert!(serde_json::from_value::<AgentGetResponse>(malformed).is_err());
    let mut malformed = wire;
    malformed["pending"] = json!([["request", "a1", 0, "turn", "approval", {}]]);
    assert!(serde_json::from_value::<AgentGetResponse>(malformed).is_err());
    let reply = AgentListResponse {
        state_totals: None,
        agents: vec![AgentListRow {
            summary: summary(),
            observation: None,
        }],
        next: Some("a1".to_owned()),
        total: 1,
        truncated: false,
    };
    let wire = serde_json::to_value(&reply).unwrap();
    assert!(wire["agents"][0].get("observation").is_none());
    assert_eq!(
        serde_json::from_value::<AgentListResponse>(wire.clone()).unwrap(),
        reply
    );
    AgentListRequest::default()
        .validate_response(&reply)
        .unwrap();
    // Compact objects reject nested positional arrays; full Agent rows
    // stay on `agent.get`.
    let mut malformed = wire.clone();
    malformed["agents"][0]["activity"] = json!([["running"]]);
    assert!(serde_json::from_value::<AgentListResponse>(malformed).is_err());
    let mut malformed = wire;
    malformed["agents"][0]["usage"] = json!([["message", "m1"]]);
    assert!(serde_json::from_value::<AgentListResponse>(malformed).is_err());
}

fn summary() -> AgentSummary {
    AgentSummary {
        archived: false,
        chat: false,

        id: "a1".to_owned(),
        generation: 0,
        project: "p".to_owned(),
        repo_common_dir: None,
        workspace: None,
        harness: "fake".to_owned(),
        name: None,
        profile: None,
        preview: None,
        state: AgentState::Idle,
        parent: None,
        root: "a1".to_owned(),
        active_turn: None,
        last_progress_at: None,
        progress_age_ms: None,
        last_turn_id: None,
        last_outcome: None,
        last_resolution_source: None,
        activity: None,
        usage: None,
        usage_scope: None,
        updated_at: 7,
        project_scope: None,
        heavy: None,
        session_growth: None,
    }
}

#[test]
fn summary_title_sources_are_optional_nonblank_and_bounded() {
    let mut row = summary();
    let wire = serde_json::to_value(&row).unwrap();
    for absent in ["name", "profile", "preview"] {
        assert!(wire.get(absent).is_none(), "{absent}");
    }
    row.name = Some("planner".to_owned());
    row.profile = Some("codex-lite".to_owned());
    row.preview = Some("é".repeat(AGENT_PREVIEW_CHARS));
    row.validate().unwrap();
    let read: AgentSummary = serde_json::from_value(serde_json::to_value(&row).unwrap()).unwrap();
    assert_eq!(read, row);
    row.preview = Some("x".repeat(AGENT_PREVIEW_CHARS + 1));
    assert!(row.validate().is_err());
    row.preview = Some(
        " 
 "
        .to_owned(),
    );
    assert!(row.validate().is_err());
    row.preview = None;
    row.name = Some(String::new());
    assert!(row.validate().is_err());
}

#[test]
fn summary_usage_scope_binds_usage_provenance() {
    let mut scoped = summary();
    // Absent usage carries no scope.
    scoped.validate().unwrap();
    assert!(
        serde_json::to_value(&scoped)
            .unwrap()
            .get("usage_scope")
            .is_none()
    );
    // Turn-backed usage carries the turn scope.
    scoped.usage = Some(TurnUsage {
        scope: Some("turn".to_owned()),
        message_id: None,
        input_tokens: Some(10),
        output_tokens: None,
        reasoning_tokens: None,
        thinking_tokens: None,
        cache_read_tokens: None,
        cache_write_tokens: None,
        total_tokens: None,
    });
    scoped.usage_scope = Some("turn".to_owned());
    scoped.validate().unwrap();
    // Session fallback carries the session scope, never a turn scope.
    scoped.usage_scope = Some("session".to_owned());
    scoped.usage.as_mut().unwrap().scope = Some("session".to_owned());
    scoped.validate().unwrap();
    let wire = serde_json::to_value(&scoped).unwrap();
    assert_eq!(wire["usage_scope"], "session");
    // Scope without usage, usage without scope, and unknown scopes fail.
    for (usage, scope) in [
        (None, Some("turn".to_owned())),
        (scoped.usage.clone(), None),
        (scoped.usage.clone(), Some("message".to_owned())),
    ] {
        let mut bad = summary();
        bad.usage = usage;
        bad.usage_scope = scope;
        assert!(bad.validate().is_err());
    }
}

fn turn_usage(input: Option<i64>, output: Option<i64>, total: Option<i64>) -> TurnUsage {
    TurnUsage {
        scope: Some("turn".to_owned()),
        message_id: None,
        input_tokens: input,
        output_tokens: output,
        reasoning_tokens: None,
        thinking_tokens: None,
        cache_read_tokens: None,
        cache_write_tokens: None,
        total_tokens: total,
    }
}

#[test]
fn fleet_usage_reports_tokens_with_turn_or_session_provenance() {
    // No usage reads as absent.
    assert_eq!(summary().fleet_usage(), None);
    // Native totals keep the turn scope.
    let mut scoped = summary();
    scoped.usage = Some(turn_usage(Some(10), Some(5), Some(15)));
    scoped.usage_scope = Some("turn".to_owned());
    assert_eq!(scoped.fleet_usage(), Some((15, "turn")));
    // Without a native total the input+output sum is the fleet signal.
    scoped.usage = Some(turn_usage(Some(2), Some(4), None));
    assert_eq!(scoped.fleet_usage(), Some((6, "turn")));
    // The session fallback never presents as turn usage.
    let mut session = turn_usage(Some(47_024), Some(207), None);
    session.scope = Some("session".to_owned());
    scoped.usage = Some(session);
    scoped.usage_scope = Some("session".to_owned());
    assert_eq!(scoped.fleet_usage(), Some((47_231, "session")));
}

#[test]
fn observed_display_and_bucket_follow_activity_not_lifecycle_state() {
    use crate::NativeChildActivity;
    use crate::NativeChildActivityState as Activity;
    let observed = |state: Activity| {
        let mut row = summary();
        row.state = AgentState::Unknown;
        row.activity = Some(NativeChildActivity {
            state,
            has_open_turn: matches!(state, Activity::Running | Activity::Lost),
            last_progress_at: None,
        });
        row
    };
    assert_eq!(observed(Activity::Idle).display_state(), "idle (observed)");
    let mut ended = observed(Activity::Idle);
    ended.state = AgentState::Stopped;
    assert_eq!(ended.display_state(), "stopped (ended with parent)");
    assert_eq!(ended.fleet_bucket(), "stopped");
    assert_eq!(
        observed(Activity::Running).display_state(),
        "running (observed)"
    );
    assert_eq!(
        observed(Activity::Interrupted).display_state(),
        "interrupted (observed)"
    );
    assert_eq!(
        observed(Activity::Unknown).display_state(),
        "unknown (observed)"
    );
    assert_eq!(observed(Activity::Lost).display_state(), "lost (observed)");
    assert_eq!(summary().display_state(), "idle");
    assert_eq!(observed(Activity::Idle).fleet_bucket(), "idle");
    assert_eq!(observed(Activity::Running).fleet_bucket(), "running");
    assert_eq!(
        observed(Activity::Interrupted).fleet_bucket(),
        "interrupted"
    );
    assert_eq!(observed(Activity::Unknown).fleet_bucket(), "unknown");
    assert_eq!(
        (
            activity_state_name(Activity::Lost),
            observed(Activity::Lost).fleet_bucket()
        ),
        ("lost", "unknown")
    );
    assert_eq!(summary().fleet_bucket(), "idle");
    let mut unknown = summary();
    unknown.state = AgentState::Unknown;
    assert_eq!(unknown.fleet_bucket(), "unknown");
    let wire = serde_json::to_value(observed(Activity::Idle)).unwrap();
    assert_eq!(wire["state"], "unknown");
    assert_eq!(wire["activity"]["state"], "idle");
}

#[test]
fn list_context_allows_self_principal_and_checks_projection_and_cursor() {
    let mut request = AgentListRequest {
        after: Some("a9".to_owned()),
        ..Default::default()
    };
    let mut reply = AgentListResponse {
        state_totals: None,
        agents: vec![AgentListRow {
            summary: summary(),
            observation: None,
        }],
        next: Some("a1".to_owned()),
        total: 1,
        truncated: false,
    };
    request.validate_response(&reply).unwrap(); // self-scope keeps one row with its cursor.
    reply.next = Some("wrong".to_owned());
    unknown(request.validate_response(&reply));
    reply.next = Some("a1".to_owned());
    request.after = Some("a1".to_owned());
    unknown(request.validate_response(&reply));
    request.after = None;
    request.observations = true;
    unknown(request.validate_response(&reply));
    reply.agents[0].observation = Some(observation());
    request.validate_response(&reply).unwrap();
    reply.agents.push(reply.agents[0].clone());
    unknown(request.validate_response(&reply));
    reply.agents[1].summary.id = "a2".to_owned();
    reply.agents[1].summary.root = "a2".to_owned();
    reply.next = Some("a2".to_owned());
    request.limit = 1;
    unknown(request.validate_response(&reply));
}

#[test]
fn list_filters_match_projection_and_report_counts_honestly() {
    let request = AgentListRequest {
        project: Some("p".to_owned()),
        harness: Some("fake".to_owned()),
        state: Some(AgentState::Idle),
        root: Some("a1".to_owned()),
        ..Default::default()
    };
    request.validate().unwrap();
    let reply = AgentListResponse {
        state_totals: None,
        agents: vec![AgentListRow {
            summary: summary(),
            observation: None,
        }],
        next: Some("a1".to_owned()),
        total: 1,
        truncated: false,
    };
    request.validate_response(&reply).unwrap();
    // A short page must not claim truncation; total covers the scope.
    let mut short = reply.clone();
    short.truncated = true;
    short.next = None;
    unknown(request.validate_response(&short));
    let mut undercounted = reply.clone();
    undercounted.total = 0;
    unknown(request.validate_response(&undercounted));
    // Filter mismatches reject instead of filtering silently.
    for mutate in [
        AgentListRequest {
            project: Some("other".to_owned()),
            ..request.clone()
        },
        AgentListRequest {
            harness: Some("other".to_owned()),
            ..request.clone()
        },
        AgentListRequest {
            state: Some(AgentState::Busy),
            ..request.clone()
        },
        AgentListRequest {
            parent: Some("a2".to_owned()),
            ..request.clone()
        },
        AgentListRequest {
            root: Some("a2".to_owned()),
            ..request.clone()
        },
        AgentListRequest {
            project_scope: Some("other".to_owned()),
            ..request.clone()
        },
    ] {
        unknown(mutate.validate_response(&reply));
    }
    // The scope filter reads the effective scope: a registered row whose
    // project label differs still matches its retained scope, while the
    // project filter keeps matching the label.
    let mut registered = reply.clone();
    registered.agents[0].summary.project = "reg".to_owned();
    registered.agents[0].summary.project_scope = Some("p".to_owned());
    let scoped = AgentListRequest {
        project_scope: Some("p".to_owned()),
        harness: Some("fake".to_owned()),
        state: Some(AgentState::Idle),
        root: Some("a1".to_owned()),
        ..Default::default()
    };
    scoped.validate_response(&registered).unwrap();
    unknown(request.validate_response(&registered));
    // Unknown filter shapes are invalid, never empty matches.
    for invalid in [
        AgentListRequest {
            project: Some("  ".to_owned()),
            ..Default::default()
        },
        AgentListRequest {
            project_scope: Some(String::new()),
            ..Default::default()
        },
        AgentListRequest {
            harness: Some(String::new()),
            ..Default::default()
        },
        AgentListRequest {
            parent: Some("a0".to_owned()),
            ..Default::default()
        },
        AgentListRequest {
            root: Some("nope".to_owned()),
            ..Default::default()
        },
    ] {
        assert!(invalid.validate().is_err());
    }
    assert!(serde_json::from_value::<AgentListRequest>(json!({"state": "done"})).is_err());
}

#[test]
fn default_list_admits_only_ended_archived_rows() {
    let row = |state, archived| {
        let mut summary = summary();
        summary.state = state;
        summary.archived = archived;
        AgentListResponse {
            state_totals: None,
            agents: vec![AgentListRow {
                summary,
                observation: None,
            }],
            next: Some("a1".to_owned()),
            total: 1,
            truncated: false,
        }
    };
    let default = AgentListRequest::default();
    // An archived ended Agent with unresolved custody stays in the default
    // list; an archived live one never does.
    for state in [AgentState::Lost, AgentState::Failed, AgentState::Stopped] {
        default.validate_response(&row(state, true)).unwrap();
    }
    unknown(default.validate_response(&row(AgentState::Idle, true)));
    let only = AgentListRequest {
        archived: crate::api::ArchivedFilter::Only,
        ..Default::default()
    };
    only.validate_response(&row(AgentState::Lost, true))
        .unwrap();
    unknown(only.validate_response(&row(AgentState::Lost, false)));
}

#[test]
fn list_pagination_requires_cursor_on_every_nonempty_page() {
    let request = AgentListRequest::default();
    let row = || AgentListRow {
        summary: summary(),
        observation: None,
    };
    // Every nonempty page carries `next` at the last returned id, even
    // when nothing is truncated; an empty page carries no cursor.
    let ok = AgentListResponse {
        state_totals: None,
        agents: vec![row()],
        next: Some("a1".to_owned()),
        total: 1,
        truncated: false,
    };
    request.validate_response(&ok).unwrap();
    let mut missing_cursor = ok.clone();
    missing_cursor.next = None;
    unknown(request.validate_response(&missing_cursor));
    let mut wrong_cursor = ok.clone();
    wrong_cursor.next = Some("a2".to_owned());
    unknown(request.validate_response(&wrong_cursor));
    let empty_ok = AgentListResponse {
        state_totals: None,
        agents: vec![],
        next: None,
        total: 1,
        truncated: false,
    };
    request.validate_response(&empty_ok).unwrap();
    let mut dangling = empty_ok.clone();
    dangling.next = Some("a1".to_owned());
    unknown(request.validate_response(&dangling));
    // `total` and `truncated` are required on the wire: a missing count
    // never reads back as zero, and a missing flag never reads as quiet.
    let wire = serde_json::to_value(&ok).unwrap();
    for field in ["total", "truncated"] {
        let mut missing = wire.clone();
        missing.as_object_mut().unwrap().remove(field);
        assert!(
            serde_json::from_value::<AgentListResponse>(missing).is_err(),
            "{field} must be required"
        );
    }
}

#[test]
fn process_response_keeps_owner_generation_order_and_cursor_binding() {
    let mut request = AgentProcessesRequest::new("a1");
    request.scope = Some("workspace".to_owned());
    let mut reply = AgentProcessesResponse {
        agent: "a1".to_owned(),
        generation: 1,
        scope: Some("workspace".to_owned()),
        processes: vec![process()],
        next: Some(cursor().encode().unwrap()),
    };
    request.validate_response(&reply).unwrap();
    let wire = serde_json::to_value(&reply).unwrap();
    assert!(wire["processes"][0]["exit_code"].is_null());
    assert_eq!(
        serde_json::from_value::<AgentProcessesResponse>(wire.clone()).unwrap(),
        reply
    );
    let mut malformed = wire.clone();
    malformed["processes"][0]["identity"] = json!(["boot", 7, 9]);
    assert!(serde_json::from_value::<AgentProcessesResponse>(malformed).is_err());
    let mut malformed = wire;
    malformed["processes"] = json!([[]]);
    assert!(serde_json::from_value::<AgentProcessesResponse>(malformed).is_err());
    request.generation = Some(2);
    unknown(request.validate_response(&reply));
    request.generation = None;
    reply.processes[0].owner = "agent:a1#1".to_owned();
    unknown(request.validate_response(&reply));
    reply.processes[0] = process();
    request.after = reply.next.clone();
    unknown(request.validate_response(&reply));
    request.after = None;
    reply.processes.push(process());
    unknown(request.validate_response(&reply));
    reply.processes.pop();
    reply.next = None;
    unknown(request.validate_response(&reply));
    reply.processes.clear();
    request.validate_response(&reply).unwrap();
    reply.next = Some(cursor().encode().unwrap());
    unknown(request.validate_response(&reply));
}

#[test]
fn stats_git_edits_bind_explicit_generation_and_keep_flattened_failures() {
    let mut request = AgentStatsRequest::new("a1");
    let reply = AgentStatsResponse {
        agent: "a1".to_owned(),
        generation: 4,
        observation: observation(),
        session_usage: None,
        heavy: None,
        session_growth: None,
    };
    let wire = serde_json::to_value(&reply).unwrap();
    assert_eq!(wire["available"], false);
    assert!(wire.get("observation").is_none());
    assert_eq!(
        serde_json::from_value::<AgentStatsResponse>(wire).unwrap(),
        reply
    );
    request.validate_response(&reply).unwrap();
    request.generation = Some(3);
    unknown(request.validate_response(&reply));
    let reply: AgentGitResponse = serde_json::from_value(
        json!({"agent":"a1","generation":4,"available":false,"error_code":"unavailable"}),
    )
    .unwrap();
    let mut request = AgentGitRequest::new("a1");
    request.validate_response(&reply).unwrap();
    assert_eq!(
        serde_json::to_value(&reply).unwrap()["error_code"],
        "unavailable"
    );
    request.id = "a2".to_owned();
    unknown(request.validate_response(&reply));
    let mut request = AgentEditsRequest::new("a1");
    request.generation = Some(1);
    request.after = Some(" ".to_owned());
    let mut reply = AgentEditsResponse {
        agent: "a1".to_owned(),
        generation: 1,
        edits: vec![AgentEdit {
            agent: "a1".to_owned(),
            generation: 1,
            workspace: "opaque".to_owned(),
            path: "note.txt".to_owned(),
            updated_at: 0,
        }],
        next: Some("note.txt".to_owned()),
    };
    request.validate_response(&reply).unwrap();
    reply.edits[0].generation = 2;
    unknown(request.validate_response(&reply));
    reply.edits[0].generation = 1;
    reply.next = None;
    unknown(request.validate_response(&reply));
    reply.edits.clear();
    request.validate_response(&reply).unwrap();
}

fn status() -> Value {
    json!({"state":"running","admission":"open","agents":0,"store_write_faulted":false,
            "owned_plugins":0,"observer_readers":0,"discovery_readers":0,
            "workspace_custody":{"scope":"current_host","capability":"unix_process_group_degraded","exact":false,"orphan_possible":true},
            "workspace_recovery_error":null,"cloud_mcp":null,
            "automation_tasks":[{"name":"rule_worker","health":"running","failure_code":null},{"name":"goal_driver","health":"running","failure_code":null}],
            "unknown_commands_count":1,"unknown_commands":["c1"],"unknown_commands_truncated":false,
            "unsettled_commands_count":1,"unsettled_commands":[{"id":"c1","agent":null,"generation":null,"verb":"launch","state":"unknown"}],
            "unsettled_commands_truncated":false,"blockers":[{"kind":"workspace_processes"},{"kind":"discovery_readers","count":1},
            {"kind":"agent_live","agent":"a1","generation":0,"state":"created"}, {"kind":"command_unsettled","command_id":"c1","state":"unknown"},
            {"kind":"store_write_faulted"},{"kind":"command_unknown","command_id":"c2"},{"kind":"command_unknown_more","count":1}]})
}

#[test]
fn daemon_status_keeps_all_typed_blockers_nullable_fields_and_actual_modes() {
    let request = DaemonStatusRequest::default();
    let wire = status();
    let mut reply: DaemonStatusResponse = serde_json::from_value(wire.clone()).unwrap();
    request.validate_response(&reply).unwrap();
    assert_eq!(serde_json::to_value(&reply).unwrap(), wire);
    for mode in ["draining", "forcing"] {
        reply.admission = mode.to_owned();
        reply.state = mode.to_owned();
        request.validate_response(&reply).unwrap();
    }
    reply.state = "running".to_owned();
    unknown(request.validate_response(&reply));
    for field in ["workspace_custody", "unsettled_commands", "blockers"] {
        let mut malformed = wire.clone();
        malformed[field] = if field == "workspace_custody" {
            json!([])
        } else {
            json!([[]])
        };
        assert!(serde_json::from_value::<DaemonStatusResponse>(malformed).is_err());
    }
    for field in [
        "workspace_recovery_error",
        "observer_readers",
        "unsettled_commands",
        "blockers",
    ] {
        let mut missing = wire.clone();
        missing.as_object_mut().unwrap().remove(field);
        assert!(serde_json::from_value::<DaemonStatusResponse>(missing).is_err());
    }
    // Drain report is optional for backward compatibility but validated
    // when present; AgentLive keeps its active turn when observed.
    let mut with_drain = wire.clone();
    with_drain["drain"] = json!({
        "running_turns_count":1,
        "running_turns":[{"agent":"a1","generation":0,"turn_id":"t1"}],
        "running_turns_truncated":false,
        "unsettled_commands_count":1,
        "unsettled_commands":[{"id":"c1","agent":null,"generation":null,"verb":"launch","state":"unknown"}],
        "unsettled_commands_truncated":false,
        "unknown_commands_count":1,"unknown_commands":["c1"],"unknown_commands_truncated":false,
        "owned_processes_count":1,
        "owned_processes":[{"agent":"a1","generation":0,"kind":"plugin"}],
        "owned_processes_truncated":false,
        "retained_unresolved_count":1,
        "retained_unresolved":[{"id":"c1","verb":"attach","agent":"a1","generation":1,
            "reason":"observer_detach_fence","resolvable":false,
            "hint":"not owner-resolvable (observe-only commands cannot be resolved); retained for the record, no action needed; use --force only for live work"}],
        "retained_unresolved_truncated":false
    });
    let reply: DaemonStatusResponse = serde_json::from_value(with_drain.clone()).unwrap();
    request.validate_response(&reply).unwrap();
    assert_eq!(serde_json::to_value(&reply).unwrap(), with_drain);
    let mut live = with_drain.clone();
    live["blockers"][2] =
        json!({"kind":"agent_live","agent":"a1","generation":0,"state":"idle","active_turn":"t1"});
    let reply: DaemonStatusResponse = serde_json::from_value(live.clone()).unwrap();
    request.validate_response(&reply).unwrap();
    assert_eq!(serde_json::to_value(&reply).unwrap(), live);
}

#[test]
fn drain_report_validates_counts_lists_and_truncation() {
    let mut report = DrainReport::empty();
    report.validate().unwrap();
    let wire = serde_json::to_value(&report).unwrap();
    let back: DrainReport = serde_json::from_value(wire.clone()).unwrap();
    assert_eq!(back, report);
    assert_eq!(serde_json::to_value(back).unwrap(), wire);
    // Counts must cover their lists; truncation permits more beyond bound.
    report.running_turns_count = 1;
    assert!(report.validate().is_err());
    report.running_turns.push(RunningTurnSummary {
        agent: "a1".to_owned(),
        generation: 0,
        turn_id: "t1".to_owned(),
    });
    report.validate().unwrap();
    report.running_turns_truncated = true;
    report.running_turns_count = 0;
    assert!(report.validate().is_err());
    report.running_turns_count = 2;
    report.validate().unwrap();
}

fn retained_unknown() -> RetainedUnknownSummary {
    RetainedUnknownSummary {
        id: "c1".to_owned(),
        verb: "attach".to_owned(),
        agent: Some("a1".to_owned()),
        generation: Some(1),
        reason: "observer_detach_fence".to_owned(),
        resolvable: false,
        hint: "not owner-resolvable; retained for the record".to_owned(),
    }
}

#[test]
fn drain_report_retained_unknowns_validate_shape_and_counts() {
    let mut report = DrainReport::empty();
    report.retained_unresolved_count = 1;
    assert!(report.validate().is_err());
    report.retained_unresolved.push(retained_unknown());
    report.validate().unwrap();
    for mut bad in [
        retained_unknown(),
        retained_unknown(),
        retained_unknown(),
        retained_unknown(),
    ] {
        bad.id = " ".to_owned();
        assert!(bad.validate().is_err());
        bad = retained_unknown();
        bad.reason = " ".to_owned();
        assert!(bad.validate().is_err());
        bad = retained_unknown();
        bad.hint = "x".repeat(257);
        assert!(bad.validate().is_err());
        bad = retained_unknown();
        bad.generation = Some(-1);
        assert!(bad.validate().is_err());
    }
    let wire = serde_json::to_value(&report).unwrap();
    let back: DrainReport = serde_json::from_value(wire.clone()).unwrap();
    assert_eq!(back, report);
    assert_eq!(serde_json::to_value(back).unwrap(), wire);
}

fn retention_view() -> RetentionView {
    RetentionView {
        events_days: None,
        commands_days: None,
        mail_days: None,
        pruned_through: 7,
        db_bytes: 1024,
        wal_bytes: 0,
        unbounded: true,
        size_advisory: None,
        config_error: None,
    }
}

#[test]
fn unreadable_config_reads_back_unknown_windows_never_unbounded() {
    let unknown = RetentionView {
        unbounded: false,
        config_error: Some("invalid: config.toml could not be parsed".to_owned()),
        ..retention_view()
    };
    unknown.validate().unwrap();
    let wire = serde_json::to_value(&unknown).unwrap();
    assert_eq!(
        wire["config_error"],
        "invalid: config.toml could not be parsed"
    );
    assert_eq!(
        serde_json::from_value::<RetentionView>(wire).unwrap(),
        unknown
    );
    // A failed read is never presented as an unset (unbounded) policy,
    // with an advisory, or with windows it could not have read.
    for bad in [
        RetentionView {
            unbounded: true,
            ..unknown.clone()
        },
        RetentionView {
            size_advisory: Some("growing".to_owned()),
            ..unknown.clone()
        },
        RetentionView {
            events_days: Some(30),
            ..unknown.clone()
        },
        RetentionView {
            config_error: Some(" ".to_owned()),
            ..unknown.clone()
        },
    ] {
        assert!(bad.validate().is_err());
    }
    // Readable configuration omits the field on the wire.
    assert!(
        serde_json::to_value(retention_view())
            .unwrap()
            .get("config_error")
            .is_none()
    );
}

#[test]
fn retention_view_validates_windows_and_advisory() {
    retention_view().validate().unwrap();
    let mut bounded = retention_view();
    bounded.events_days = Some(30);
    bounded.unbounded = false;
    bounded.validate().unwrap();
    for mut bad in [
        retention_view(),
        retention_view(),
        retention_view(),
        retention_view(),
        retention_view(),
    ] {
        bad.events_days = Some(0);
        bad.unbounded = false;
        assert!(bad.validate().is_err());
        bad = retention_view();
        bad.unbounded = false;
        assert!(bad.validate().is_err());
        bad = retention_view();
        bad.pruned_through = -1;
        assert!(bad.validate().is_err());
        bad = retention_view();
        bad.size_advisory = Some("growing".to_owned());
        assert!(bad.validate().is_ok());
        bad = retention_view();
        bad.events_days = Some(30);
        bad.size_advisory = Some("growing".to_owned());
        assert!(bad.validate().is_err());
    }
    let wire = serde_json::to_value(retention_view()).unwrap();
    let back: RetentionView = serde_json::from_value(wire.clone()).unwrap();
    assert_eq!(back, retention_view());
    // Retention stays optional for backward compatibility: an older
    // reply without it still validates.
    let request = DaemonStatusRequest::default();
    let mut reply: DaemonStatusResponse = serde_json::from_value(status()).unwrap();
    request.validate_response(&reply).unwrap();
    reply.retention = Some(retention_view());
    request.validate_response(&reply).unwrap();
    let mut bad = reply.clone();
    bad.retention = Some(RetentionView {
        unbounded: false,
        ..retention_view()
    });
    assert!(request.validate_response(&bad).is_err());
}

#[test]
fn source_projection_retains_null_counters_and_pending_workspace_generation() {
    let wire = json!({"agent_id":"a1","generation":1,"source":"workspace_process","families":["workspace_process"],
            "owners":[{"family":"workspace_process","source":"workspace_process","mechanism":"os"}],"state":"open","health":"ok",
            "cursor":null,"last_store_seq":null,"last_batch_id":null,"last_batch_first_seq":null,"last_batch_last_seq":null,
            "gap_id":null,"gap_from":null,"gap_to":null,"gap_seq":null});
    let source: SourceState = serde_json::from_value(wire.clone()).unwrap();
    assert_eq!(serde_json::to_value(&source).unwrap(), wire);
    let mut reply = AgentGetResponse {
        agent: Agent::new("a1", "p", "fake", "fake", 0),
        observation: observation(),
        source_states: vec![source],
        session_usage: None,
        config: None,
        heavy: None,
        session_growth: None,
    };
    AgentGetRequest::new("a1")
        .validate_response(&reply)
        .unwrap();
    reply.source_states[0].agent_id = "a2".to_owned();
    unknown(AgentGetRequest::new("a1").validate_response(&reply));
    let mut malformed = wire.clone();
    malformed["owners"] = json!([["workspace_process", "workspace_process", "os"]]);
    assert!(serde_json::from_value::<SourceState>(malformed).is_err());
    let mut missing = wire;
    missing.as_object_mut().unwrap().remove("last_store_seq");
    assert!(serde_json::from_value::<SourceState>(missing).is_err());
}

#[test]
fn multi_state_list_requires_complete_scoped_totals_and_a_bounded_page() {
    let request = AgentListRequest {
        states: Some(vec![AgentState::Idle, AgentState::Busy]),
        limit: 1,
        ..Default::default()
    };
    let mut reply = AgentListResponse {
        state_totals: Some(
            AGENT_LIST_STATES
                .into_iter()
                .map(|state| AgentStateTotal {
                    state,
                    total: match state {
                        AgentState::Idle => 2,
                        AgentState::Busy => 3,
                        AgentState::Stopped => 20,
                        _ => 0,
                    },
                })
                .collect(),
        ),
        agents: vec![AgentListRow {
            summary: summary(),
            observation: None,
        }],
        total: 5,
        truncated: true,
        next: Some("a1".to_owned()),
    };
    request.validate_response(&reply).unwrap();
    let good = reply.clone();
    reply.state_totals = None;
    unknown(request.validate_response(&reply));
    reply = good.clone();
    reply.state_totals.as_mut().unwrap()[0].state = AgentState::Idle;
    unknown(request.validate_response(&reply));
    reply = good.clone();
    reply.total = 4;
    unknown(request.validate_response(&reply));
    reply = good.clone();
    reply.agents[0].summary.state = AgentState::Stopped;
    unknown(request.validate_response(&reply));
    reply = good;
    reply.agents.push(reply.agents[0].clone());
    unknown(request.validate_response(&reply));
    for states in [vec![], vec![AgentState::Busy, AgentState::Busy]] {
        assert!(
            AgentListRequest {
                states: Some(states),
                ..Default::default()
            }
            .validate()
            .is_err()
        );
    }
    assert!(
        AgentListRequest {
            state: Some(AgentState::Idle),
            ..request
        }
        .validate()
        .is_err()
    );
    assert!(serde_json::from_value::<AgentListRequest>(json!({"states":["busy","done"]})).is_err());
}

#[test]
fn list_recent_validates_activity_order_and_stable_cursor() {
    use crate::api::{AgentListOrder, AgentListRequest};
    use crate::api::{decode_recent_cursor, encode_recent_cursor, recent_activity};
    let mut first = summary();
    first.id = "a2".to_owned();
    first.root = "a2".to_owned();
    first.updated_at = 200;
    first.last_progress_at = None;
    let mut second = summary();
    second.id = "a10".to_owned();
    second.root = "a10".to_owned();
    second.updated_at = 100;
    second.last_progress_at = None;
    let request = AgentListRequest {
        order: AgentListOrder::Recent,
        ..Default::default()
    };
    request.validate().unwrap();
    let reply = AgentListResponse {
        state_totals: None,
        agents: vec![
            AgentListRow {
                summary: first.clone(),
                observation: None,
            },
            AgentListRow {
                summary: second.clone(),
                observation: None,
            },
        ],
        next: Some(encode_recent_cursor(
            &second.id,
            recent_activity(second.last_progress_at, second.updated_at),
        )),
        total: 2,
        truncated: false,
    };
    request.validate_response(&reply).unwrap();
    let (id, activity) = decode_recent_cursor(reply.next.as_deref().unwrap()).unwrap();
    assert_eq!(id, "a10");
    assert_eq!(activity, 100);
    // Wrong order (ascending) rejects.
    let mut swapped = reply.clone();
    swapped.agents.swap(0, 1);
    swapped.next = Some(encode_recent_cursor(
        &first.id,
        recent_activity(first.last_progress_at, first.updated_at),
    ));
    assert!(request.validate_response(&swapped).is_err());
    // Legacy id cursor rejects in recent order.
    assert!(
        AgentListRequest {
            order: AgentListOrder::Recent,
            after: Some("a10".to_owned()),
            ..Default::default()
        }
        .validate()
        .is_err()
    );
    // Id order keeps the last-id cursor.
    let id_request = AgentListRequest::default();
    assert_eq!(id_request.order, AgentListOrder::Id);
    let id_reply = AgentListResponse {
        state_totals: None,
        agents: vec![AgentListRow {
            summary: second,
            observation: None,
        }],
        next: Some("a10".to_owned()),
        total: 1,
        truncated: false,
    };
    id_request.validate_response(&id_reply).unwrap();
}
