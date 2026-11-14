use std::collections::BTreeMap;

use eliot_core::{ProcessIdentity, ProcessRecord, ProcessState};
use eliot_store::Store;

/// Install the fixture native source plan after dispatch, before any native
/// batch. Fixture counterpart of the PluginHost manifest validation
/// (`install_plugin_sources` after manifest check, before `agent.start`);
/// the ownership gate refuses every native family until this plan commits.
/// Uses the common five-family stream plan so every native kind used in
/// fixtures has an owner.
pub(crate) fn install_fixture_sources(
    store: &mut Store,
    agent: &str,
    start_command_id: &str,
    generation: i64,
    now: i64,
) {
    install_fixture_source_plan(
        store,
        agent,
        start_command_id,
        generation,
        now,
        &["session", "turn", "message", "tool", "request"],
    );
}

/// Install a scoped fixture native source plan. Handoff fixtures install
/// exactly the families their handoffs move so the predecessor retains
/// nothing outside the handoff universe.
pub(crate) fn install_fixture_source_plan(
    store: &mut Store,
    agent: &str,
    start_command_id: &str,
    generation: i64,
    now: i64,
    families: &[&str],
) {
    let plan: BTreeMap<String, String> = families
        .iter()
        .map(|family| ((*family).to_owned(), "stream".to_owned()))
        .collect();
    store
        .install_plugin_sources(agent, generation, start_command_id, &plan, now)
        .expect("install fixture source plan");
}

/// Seed a live OS source and owned-plugin record through Store's public API.
pub(crate) fn install_live_owned_process_source(
    store: &mut Store,
    agent: &str,
    generation: i64,
    now: i64,
) {
    store
        .ensure_process_source(agent, generation, now)
        .expect("install live owned-process source");
    store
        .observe_processes(
            agent,
            generation,
            &[ProcessRecord {
                identity: simulated_process_identity(agent, generation),
                owner: format!("agent:{agent}#{generation}"),
                role: "plugin".to_owned(),
                state: ProcessState::Running,
                exit_code: None,
                updated_at: now,
            }],
            now,
        )
        .expect("observe live owned plugin");
}

/// Simulate normal host and monitor exit proof after the terminal event.
pub(crate) fn simulate_owned_process_exit_proof(
    store: &mut Store,
    agent: &str,
    generation: i64,
    now: i64,
) {
    store
        .observe_processes(
            agent,
            generation,
            &[ProcessRecord {
                identity: simulated_process_identity(agent, generation),
                owner: format!("agent:{agent}#{generation}"),
                role: "plugin".to_owned(),
                state: ProcessState::Exited,
                exit_code: Some(0),
                updated_at: now,
            }],
            now,
        )
        .expect("observe owned plugin exit");
    store
        .close_native_sources(agent, generation, now)
        .expect("close drained native sources");
    store
        .set_process_source_status(agent, generation, "ok", true, now)
        .expect("close OS process source after exit");
    let events = store
        .record_owned_control_exit(agent, generation, now)
        .expect("record owned control exit proof");
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == "capacity.released")
            .count(),
        1,
        "complete exit proof releases the fixture generation exactly once"
    );
}

// Keep one stable OS identity across the running and exited observations.
fn simulated_process_identity(agent: &str, generation: i64) -> ProcessIdentity {
    ProcessIdentity {
        host_boot_id: format!("simulated-os-{agent}"),
        pid: 81_001,
        start_time: u64::try_from(generation).expect("positive fixture generation"),
    }
}
