use super::subscriptions::Category;
use super::{TOOLS, find_tool, profiles, tool_error, tool_name};
use crate::config::{McpConfig, McpProfileConfig, McpToolProfile};
use rmcp::ServerHandler;
use serde_json::{Value, json};
#[test]
fn profile_tables_are_closed_and_keep_gm_authority_separate() {
    let expected_observer: std::collections::BTreeSet<&str> = [
        "swarm.tools.search",
        "swarm.dashboard",
        "host.status",
        "task.get",
        "task.list",
        "task.submission",
        "task.acceptance",
        "attempt.get",
        "operation.get",
        "operation.list",
        "concilium.get",
        "concilium.list",
        "agent.state",
        "agent.list",
        "agent.family",
        "check.get",
        "check.profiles",
        "artifact.get",
        "artifact.read",
        "artifact.parts",
        "report.delta",
        "report.attention",
        "report.capacity",
        "message.read",
    ]
    .into_iter()
    .collect();
    let actual_observer: std::collections::BTreeSet<&str> = TOOLS
        .iter()
        .filter(|(_, spec)| profiles::exposes_method(McpToolProfile::Observer, spec.method))
        .map(|(_, spec)| spec.method)
        .collect();
    assert_eq!(actual_observer, expected_observer);
    for method in &actual_observer {
        assert!(find_tool(&tool_name(method)).unwrap().0, "{method}");
    }

    for (_, spec) in TOOLS.iter().filter(|(read_only, _)| !*read_only) {
        assert!(
            !profiles::exposes_method(McpToolProfile::Observer, spec.method),
            "observer unexpectedly exposes mutation {}",
            spec.method
        );
    }
    assert!(profiles::exposes_method(
        McpToolProfile::Reviewer,
        "task.request_changes"
    ));
    assert!(!profiles::exposes_method(
        McpToolProfile::Reviewer,
        "task.accept"
    ));
    assert!(profiles::exposes_method(
        McpToolProfile::Manager,
        "message.cancel"
    ));
    assert!(profiles::exposes_method(
        McpToolProfile::Manager,
        "schedule.run_now"
    ));
    assert!(profiles::exposes_method(
        McpToolProfile::Gm,
        "schedule.run_now"
    ));
    for profile in [
        McpToolProfile::Observer,
        McpToolProfile::Reviewer,
        McpToolProfile::Participant,
    ] {
        assert!(!profiles::exposes_method(profile, "schedule.run_now"));
    }
    assert!(profiles::exposes_method(
        McpToolProfile::Manager,
        "agent.background"
    ));
    assert!(profiles::exposes_method(
        McpToolProfile::Manager,
        "swarm.launch.preview"
    ));
    assert!(profiles::exposes_method(
        McpToolProfile::Manager,
        "swarm.launch"
    ));
    assert!(profiles::exposes_method(
        McpToolProfile::Manager,
        "coordination.watch.create"
    ));
    assert!(profiles::exposes_method(
        McpToolProfile::Manager,
        "github.effect.managed_label"
    ));
    assert!(profiles::exposes_method(
        McpToolProfile::Gm,
        "github.effect.managed_label"
    ));
    assert!(profiles::exposes_method(
        McpToolProfile::Gm,
        "github.effect.reconcile_managed_label"
    ));
    for profile in [
        McpToolProfile::Manager,
        McpToolProfile::Observer,
        McpToolProfile::Reviewer,
        McpToolProfile::Participant,
        McpToolProfile::AssignedReviewer,
    ] {
        assert!(
            !profiles::exposes_method(profile, "github.effect.reconcile_managed_label"),
            "{profile:?} must not expose the GM-only effect recovery method"
        );
    }
    assert!(profiles::exposes_method(
        McpToolProfile::Manager,
        "github.pull_request.update_description"
    ));
    assert!(profiles::exposes_method(
        McpToolProfile::Gm,
        "github.pull_request.update_description"
    ));
    assert!(profiles::exposes_method(
        McpToolProfile::Manager,
        "github.pull_request.reconcile_description"
    ));
    assert!(profiles::exposes_method(
        McpToolProfile::Gm,
        "github.pull_request.reconcile_description"
    ));
    for profile in [
        McpToolProfile::Observer,
        McpToolProfile::Reviewer,
        McpToolProfile::Participant,
        McpToolProfile::AssignedReviewer,
    ] {
        assert!(
            !profiles::exposes_method(profile, "github.effect.managed_label"),
            "{profile:?} must not expose a GitHub write effect"
        );
        assert!(
            !profiles::exposes_method(profile, "github.pull_request.update_description"),
            "{profile:?} must not expose a GitHub pull-request write effect"
        );
        assert!(
            !profiles::exposes_method(profile, "github.pull_request.reconcile_description"),
            "{profile:?} must not expose a GitHub pull-request reconciliation effect"
        );
    }
    assert!(profiles::exposes_method(
        McpToolProfile::Manager,
        "swarm.overlap.check"
    ));
    assert!(!profiles::exposes_method(
        McpToolProfile::Manager,
        "coordination.sync_integration"
    ));
    assert!(!profiles::exposes_method(
        McpToolProfile::Manager,
        "client.register"
    ));
    assert!(!profiles::exposes_method(
        McpToolProfile::Manager,
        "host.mode"
    ));
    assert!(profiles::exposes_method(McpToolProfile::Gm, "gm.handover"));
    assert!(profiles::exposes_method(
        McpToolProfile::Gm,
        "automation.config.get"
    ));
    assert!(profiles::exposes_method(
        McpToolProfile::Gm,
        "automation.config.explain"
    ));
    assert!(profiles::exposes_method(
        McpToolProfile::Gm,
        "automation.config.transfer"
    ));
    assert!(!profiles::exposes_method(
        McpToolProfile::Gm,
        "automation.config.apply"
    ));
    assert!(profiles::exposes_method(
        McpToolProfile::Participant,
        "coordination.work_card.publish"
    ));
    assert!(profiles::exposes_method(
        McpToolProfile::Participant,
        "coordination.consult"
    ));
    assert!(profiles::exposes_method(
        McpToolProfile::Participant,
        "coordination.sync_integration"
    ));
    assert!(profiles::exposes_method(
        McpToolProfile::Participant,
        "swarm.overlap.check"
    ));
    assert!(profiles::exposes_method(
        McpToolProfile::Participant,
        "coordination.watch.create"
    ));
    assert!(profiles::exposes_method(
        McpToolProfile::Participant,
        "coordination.watch.list"
    ));
    assert!(!profiles::exposes_method(
        McpToolProfile::Participant,
        "task.get"
    ));
    assert!(!profiles::exposes_method(
        McpToolProfile::Participant,
        "swarm.launch"
    ));
    assert!(!profiles::exposes_method(
        McpToolProfile::Participant,
        "coordination.participant.list"
    ));
    assert!(profiles::exposes_method(
        McpToolProfile::AssignedReviewer,
        "review.submit"
    ));
    assert!(!profiles::exposes_method(
        McpToolProfile::AssignedReviewer,
        "task.get"
    ));
    assert!(!profiles::exposes_method(
        McpToolProfile::AssignedReviewer,
        "task.request_changes"
    ));
    assert!(!profiles::exposes_method(
        McpToolProfile::AssignedReviewer,
        "coordination.consult"
    ));
    assert!(!profiles::exposes_method(
        McpToolProfile::AssignedReviewer,
        "coordination.watch.create"
    ));
    assert!(!profiles::exposes_method(
        McpToolProfile::AssignedReviewer,
        "swarm.launch"
    ));
    assert!(!profiles::exposes_method(
        McpToolProfile::AssignedReviewer,
        "coordination.sync_integration"
    ));
    assert!(!profiles::exposes_method(
        McpToolProfile::AssignedReviewer,
        "swarm.overlap.check"
    ));
    assert!(profiles::exposes_method(
        McpToolProfile::Gm,
        "client.register"
    ));
    assert!(profiles::exposes_method(
        McpToolProfile::Full,
        "source.capture"
    ));
    assert!(!profiles::exposes_method(
        McpToolProfile::Observer,
        "not.a.public.method"
    ));
    for profile in [
        McpToolProfile::Observer,
        McpToolProfile::Reviewer,
        McpToolProfile::Manager,
        McpToolProfile::Gm,
        McpToolProfile::Full,
    ] {
        assert!(profiles::allows_subscription_category(
            profile,
            Category::Reports
        ));
        assert!(profiles::allows_subscription_category(
            profile,
            Category::Mailbox
        ));
        assert!(profiles::allows_subscription_category(
            profile,
            Category::Operations
        ));
        assert!(profiles::allows_subscription_category(
            profile,
            Category::Concilium
        ));
    }
}

#[test]
fn local_profile_binding_is_explicit_and_restricted_principals_are_distinct() {
    let config = McpConfig::default();
    config.validate().unwrap();
    assert_eq!(
        config.selected_tool_profile(None, "operator").unwrap(),
        McpToolProfile::Observer
    );
    assert_eq!(
        config
            .selected_tool_profile(Some("local-full"), "operator")
            .unwrap(),
        McpToolProfile::Full
    );
    assert_eq!(
        config
            .selected_tool_profile(Some("local-full"), "another-client")
            .unwrap_err()
            .code,
        "PROFILE_MISMATCH"
    );

    let mut duplicated = McpConfig::default();
    duplicated.profiles.insert(
        "dot-observer".into(),
        McpProfileConfig {
            tool_profile: McpToolProfile::Observer,
            expected_client_id: "same-principal".into(),
            surface: None,
            deferred_groups: Vec::new(),
            manual_tools: Vec::new(),
        },
    );
    duplicated.profiles.insert(
        "muse-observer".into(),
        McpProfileConfig {
            tool_profile: McpToolProfile::Observer,
            expected_client_id: "same-principal".into(),
            surface: None,
            deferred_groups: Vec::new(),
            manual_tools: Vec::new(),
        },
    );
    assert_eq!(duplicated.validate().unwrap_err().code, "CONFIG_ERROR");
}

#[test]
fn application_error_projection_preserves_stale_and_digest_failures() {
    for (code, message) in [
        ("STALE_REVISION", "expected revision is no longer current"),
        ("DIGEST_MISMATCH", "payload digest does not match"),
        ("UNSUPPORTED_RUNTIME", "runtime operation is unavailable"),
    ] {
        let result = tool_error(swarm_contracts::error::Error::new(code, message));
        assert_eq!(result.is_error, Some(true));
        let text = result.content[0].as_text().unwrap().text.as_str();
        let payload: Value = serde_json::from_str(text).unwrap();
        assert_eq!(payload["error"]["code"], json!(code));
        assert_eq!(payload["error"]["message"], json!(message));
    }
}

#[test]
fn extracted_public_handler_advertises_tasks() {
    let config = crate::config::Config {
        storage: super::super::config::Storage {
            data_dir: std::path::PathBuf::from("/not-opened-by-get-info"),
        },
        ipc: super::super::config::Ipc::default(),
        mcp: McpConfig::default(),
    };
    let credential = swarm_contracts::Credential {
        client_id: "operator".into(),
        token: "test-token-not-used-by-get-info".into(),
    };
    let handler = super::profiled_facade(&config, credential, Some("local-full"))
        .expect("the configured public facade is valid");
    let info = handler.get_info();
    assert!(info.capabilities.supports_tasks());
}
