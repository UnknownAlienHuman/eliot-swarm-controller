use super::*;

fn now() -> i64 {
    i64::try_from(Ulid::new().timestamp_ms()).unwrap()
}

// Only hierarchy projections are under test; no native execution is simulated.
fn agent(store: &mut Store, parent: Option<&str>) -> String {
    let mut request = SubmitCommand::new(Ulid::new().to_string(), "launch");
    request.expected_generation = Some(0);
    request.args = json!({"project":"handoff", "workspace":"hierarchy-unit", "harness":"codex", "profile":"worker", "profile_snapshot":{"control":"tree"}});
    if let Some(parent) = parent {
        request.args["parent"] = json!(parent);
    }
    let id = store
        .submit("owner", request, now())
        .unwrap()
        .command
        .target
        .unwrap();
    store
        .connection
        .execute(
            "UPDATE agents SET generation = 1, state = 'idle' WHERE id = ?1",
            [parse_agent_id(&id).unwrap()],
        )
        .unwrap();
    id
}

fn request(agent: &str, parent: &str) -> ReparentRequest {
    ReparentRequest {
        id: Ulid::new().to_string(),
        agent: Some(agent.into()),
        children_of: None,
        expected_generation: 1,
        parent: parent.into(),
        parent_generation: 1,
    }
}

#[test]
fn reparent_recomputes_subtree_and_preserves_frozen_identity() {
    let mut s = Store::open(Path::new(":memory:")).unwrap();
    let a = agent(&mut s, None);
    let child = agent(&mut s, Some(&a));
    let grandchild = agent(&mut s, Some(&child));
    let b = agent(&mut s, None);
    let manager = agent(&mut s, Some(&b));
    let before = s.get_agent(&child).unwrap().unwrap();
    let moved = s
        .reparent("owner", &request(&child, &manager), now())
        .unwrap();
    assert_eq!(moved.value.moved.len(), 2);
    assert_eq!(moved.events.len(), 2);
    let after = s.get_agent(&child).unwrap().unwrap();
    assert_eq!(after.parent.as_deref(), Some(manager.as_str()));
    assert_eq!((after.root, after.depth), (b.clone(), 2));
    assert_eq!(after.generation, before.generation);
    assert_eq!(after.profile_snapshot, before.profile_snapshot);
    assert_eq!(after.native_ref, before.native_ref);
    let gc = s.get_agent(&grandchild).unwrap().unwrap();
    assert_eq!((gc.root, gc.depth), (b, 3));
}

#[test]
fn reparent_rejects_cycles_without_writes() {
    let mut s = Store::open(Path::new(":memory:")).unwrap();
    let a = agent(&mut s, None);
    let child = agent(&mut s, Some(&a));
    let high = s.high_water().unwrap();
    for target in [&a, &child] {
        assert_eq!(
            s.reparent("owner", &request(&a, target), now())
                .unwrap_err()
                .code,
            "invalid"
        );
    }
    assert_eq!(s.high_water().unwrap(), high);
    assert_eq!(s.get_agent(&a).unwrap().unwrap().parent, None);
}

#[test]
fn reparent_changes_list_command_and_dispatch_scope() {
    let mut s = Store::open(Path::new(":memory:")).unwrap();
    let a = agent(&mut s, None);
    let child = agent(&mut s, Some(&a));
    let b = agent(&mut s, None);
    for manager in [&a, &b] {
        s.connection
            .execute(
                "UPDATE agents SET token = ?1 WHERE id = ?2",
                params![Ulid::new().to_string(), parse_agent_id(manager).unwrap()],
            )
            .unwrap();
    }
    let old = s.get_agent(&a).unwrap().unwrap();
    let new = s.get_agent(&b).unwrap().unwrap();
    assert!(s.agent_in_scope(&old, &child).unwrap());
    let mut stop = SubmitCommand::new(Ulid::new().to_string(), "stop");
    stop.target = Some(child.clone());
    stop.expected_generation = Some(1);
    let old_principal = format!("agent:{a}:1");
    s.submit(&old_principal, stop.clone(), now()).unwrap();
    s.reparent("owner", &request(&child, &b), now()).unwrap();
    assert!(!s.agent_in_scope(&old, &child).unwrap());
    assert!(s.agent_in_scope(&new, &child).unwrap());
    assert_eq!(s.dispatch(&stop.id, now()).unwrap_err().code, "not_allowed");
    stop.id = Ulid::new().to_string();
    assert_eq!(
        s.submit(&old_principal, stop.clone(), now())
            .unwrap_err()
            .code,
        "not_allowed"
    );
    s.submit(&format!("agent:{b}:1"), stop, now()).unwrap();
}

#[test]
fn reparent_checks_depth_children_tree_and_expansion_limits_atomically() {
    for bound in ["depth", "children", "tree", "expansion"] {
        let mut s = Store::open(Path::new(":memory:")).unwrap();
        let a = agent(&mut s, None);
        let child = agent(&mut s, Some(&a));
        agent(&mut s, Some(&child));
        let b = agent(&mut s, None);
        agent(&mut s, Some(&b));
        let mut limits = eliot_core::api::HierarchyLimits::default();
        match bound {
            "depth" => limits.max_depth = 1,
            "children" => limits.max_children_per_agent = 1,
            "tree" => limits.max_tree_agents = 3,
            _ => limits.max_selector_targets = 1,
        }
        s.set_hierarchy_limits(limits).unwrap();
        let high = s.high_water().unwrap();
        assert_eq!(
            s.reparent("owner", &request(&child, &b), now())
                .unwrap_err()
                .code,
            "fanout_limit",
            "{bound}"
        );
        assert_eq!(s.high_water().unwrap(), high);
        assert_eq!(s.get_agent(&child).unwrap().unwrap().parent, Some(a));
    }
}

#[test]
fn reparent_rejects_terminal_unknown_observer_parent_and_observer_subtree() {
    let mut s = Store::open(Path::new(":memory:")).unwrap();
    let a = agent(&mut s, None);
    let child = agent(&mut s, Some(&a));
    let b = agent(&mut s, None);
    for (state, observe_only, code) in [
        ("stopped", false, "bad_state"),
        ("unknown", true, "not_allowed"),
        ("idle", true, "not_allowed"),
    ] {
        s.connection
            .execute(
                "UPDATE agents SET state=?1, observe_only=?2 WHERE id=?3",
                params![state, observe_only, parse_agent_id(&b).unwrap()],
            )
            .unwrap();
        assert_eq!(
            s.reparent("owner", &request(&child, &b), now())
                .unwrap_err()
                .code,
            code
        );
    }
    s.connection
        .execute(
            "UPDATE agents SET state='idle', observe_only=0 WHERE id=?1",
            [parse_agent_id(&b).unwrap()],
        )
        .unwrap();
    let native = agent(&mut s, Some(&child));
    s.connection
        .execute(
            "UPDATE agents SET observe_only=1 WHERE id=?1",
            [parse_agent_id(&native).unwrap()],
        )
        .unwrap();
    assert_eq!(
        s.reparent("owner", &request(&child, &b), now())
            .unwrap_err()
            .code,
        "not_allowed"
    );
    assert_eq!(s.get_agent(&child).unwrap().unwrap().parent, Some(a));
}

#[test]
fn reparent_bulk_is_atomic_and_terminal_children_can_move_back() {
    let mut s = Store::open(Path::new(":memory:")).unwrap();
    let a = agent(&mut s, None);
    let one = agent(&mut s, Some(&a));
    let two = agent(&mut s, Some(&a));
    let b = agent(&mut s, None);
    let mut bulk = request(&a, &b);
    bulk.children_of = bulk.agent.take();
    assert_eq!(
        s.reparent("owner", &bulk, now()).unwrap().value.moved.len(),
        2
    );
    s.connection
        .execute(
            "UPDATE agents SET state='stopped' WHERE id=?1",
            [parse_agent_id(&one).unwrap()],
        )
        .unwrap();
    for id in [&one, &two] {
        s.reparent("owner", &request(id, &a), now()).unwrap();
    }
    assert_eq!(s.get_agent(&one).unwrap().unwrap().parent, Some(a.clone()));
    assert_eq!(s.get_agent(&two).unwrap().unwrap().parent, Some(a));
}

#[test]
fn reparent_replays_before_live_gates_and_conflicts_across_namespaces() {
    let mut s = Store::open(Path::new(":memory:")).unwrap();
    let a = agent(&mut s, None);
    let child = agent(&mut s, Some(&a));
    let b = agent(&mut s, None);
    let req = request(&child, &b);
    let first = s.reparent("owner", &req, now()).unwrap();
    s.reparent("owner", &request(&child, &a), now()).unwrap();
    s.connection
        .execute(
            "UPDATE agents SET state='stopped', generation=2 WHERE id=?1",
            [parse_agent_id(&b).unwrap()],
        )
        .unwrap();
    let replay = s.reparent("owner", &req, now()).unwrap();
    assert_eq!(replay.value, first.value);
    assert!(replay.events.is_empty());
    assert_eq!(
        s.get_agent(&child).unwrap().unwrap().parent,
        Some(a.clone())
    );
    let mut conflict = req.clone();
    conflict.parent = a.clone();
    assert_eq!(
        s.reparent("owner", &conflict, now()).unwrap_err().code,
        "id_conflict"
    );
    assert_eq!(
        s.reparent(&format!("agent:{a}:1"), &req, now())
            .unwrap_err()
            .code,
        "not_allowed"
    );
    conflict.id = Ulid::new().to_string();
    conflict.expected_generation = 2;
    assert_eq!(
        s.reparent("owner", &conflict, now()).unwrap_err().code,
        "stale_generation"
    );
    let mut reused = request(&child, &a);
    reused.id = s
        .connection
        .query_row("SELECT id FROM commands LIMIT 1", [], |row| row.get(0))
        .unwrap();
    assert_eq!(
        s.reparent("owner", &reused, now()).unwrap_err().code,
        "id_conflict"
    );
}

#[test]
fn reparent_preserves_project_scope_and_validates_source_form() {
    let mut s = Store::open(Path::new(":memory:")).unwrap();
    let a = agent(&mut s, None);
    let b = agent(&mut s, None);
    let mut req = request(&a, &b);
    req.children_of = Some(a.clone());
    assert_eq!(req.validate().unwrap_err().code, "invalid");
    req.children_of = None;
    s.connection
        .execute(
            "UPDATE agents SET project='other' WHERE id=?1",
            [parse_agent_id(&b).unwrap()],
        )
        .unwrap();
    assert_eq!(
        s.reparent("owner", &req, now()).unwrap_err().code,
        "not_allowed"
    );
}
