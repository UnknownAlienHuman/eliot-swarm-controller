use super::*;

#[tokio::test]
async fn internal_scheduler_cannot_be_registered_or_authenticated_by_transport() {
    let directory = std::env::temp_dir().join(format!("swarm-internal-auth-{}", model::new_id()));
    std::fs::create_dir(&directory).unwrap();
    let root = DataRoot::acquire(&directory).unwrap();
    let credential = crate::platform::bootstrap_credential(&root.path).unwrap();
    let mut config = Config::default();
    config.storage.data_dir = root.path.clone();
    let owner = StoreOwner::start(root, Arc::new(config), credential.clone())
        .await
        .unwrap();
    let operator = owner.store.authenticate(credential).await.unwrap();
    for (client_id, role) in [
        ("external-scheduler", "scheduler"),
        (model::INTERNAL_SCHEDULER_CLIENT_ID, "manager"),
    ] {
        let error = owner
            .store
            .call(
                operator.clone(),
                "client.register".into(),
                json!({"client_request_id":model::new_id(),"client_id":client_id,
                       "role":role,"token_hash":model::digest(b"known-transport-token")}),
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, "FORBIDDEN");
    }
    // Even adding a matching hash cannot expose an internal identity through
    // the transport credential path; the internal-only flag is authoritative.
    owner
        .store
        .run(|db| {
            set_meta(
                db,
                &format!("client:{}", model::INTERNAL_SCHEDULER_CLIENT_ID),
                &json!({"role":"scheduler","internal_only":true,"disabled":false,
                       "token_hash":model::digest(b"known-transport-token")}),
            )
        })
        .await
        .unwrap();
    let error = owner
        .store
        .authenticate(Credential {
            client_id: model::INTERNAL_SCHEDULER_CLIENT_ID.into(),
            token: "known-transport-token".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(error.code, "UNAUTHORIZED");
    assert!(error.message.contains("internal principals"));
    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn only_bootstrap_operator_is_authenticatable_and_gm_cannot_register_another() {
    let directory = std::env::temp_dir().join(format!("swarm-operator-auth-{}", model::new_id()));
    std::fs::create_dir(&directory).unwrap();
    let root = DataRoot::acquire(&directory).unwrap();
    let credential = crate::platform::bootstrap_credential(&root.path).unwrap();
    let mut config = Config::default();
    config.storage.data_dir = root.path.clone();
    let owner = StoreOwner::start(root, Arc::new(config), credential.clone())
        .await
        .unwrap();

    let operator = owner.store.authenticate(credential.clone()).await.unwrap();
    assert_eq!(operator.role, Role::Operator);
    assert_eq!(operator.client_id, credential.client_id);

    let manager_token = format!("{}{}", model::new_id(), model::new_id());
    let manager_id = "current-manager";
    owner
        .store
        .call(
            operator.clone(),
            "client.register".into(),
            json!({"client_request_id":model::new_id(),"client_id":manager_id,
                   "role":"manager","token_hash":model::digest(manager_token.as_bytes())}),
        )
        .await
        .unwrap();
    owner
        .store
        .call(
            operator.clone(),
            "gm.handover".into(),
            json!({"client_request_id":model::new_id(),"client_id":manager_id}),
        )
        .await
        .unwrap();
    let manager = owner
        .store
        .authenticate(Credential {
            client_id: manager_id.into(),
            token: manager_token,
        })
        .await
        .unwrap();
    assert_eq!(manager.role, Role::Manager);

    let remote_operator_id = "remote-operator";
    let remote_operator_token = "known-test-only-operator-token";
    let error = owner
        .store
        .call(
            manager,
            "client.register".into(),
            json!({"client_request_id":model::new_id(),"client_id":remote_operator_id,
                   "role":"operator","token_hash":model::digest(remote_operator_token.as_bytes())}),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, "FORBIDDEN");
    owner
        .store
        .run(move |db| {
            assert!(meta(db, &format!("client:{remote_operator_id}"))?.is_none());
            Ok(())
        })
        .await
        .unwrap();

    // A legacy/external Operator record must not authenticate even with a
    // matching token hash, and stale in-process principals must be rechecked.
    let legacy_operator_id = "legacy-extra-operator";
    let legacy_operator_token = "known-test-only-legacy-operator-token";
    let token_hash = model::digest(legacy_operator_token.as_bytes());
    owner
        .store
        .run(move |db| {
            set_meta(
                db,
                &format!("client:{legacy_operator_id}"),
                &json!({"role":"operator","token_hash":token_hash,"disabled":false}),
            )
        })
        .await
        .unwrap();

    let error = owner
        .store
        .authenticate(Credential {
            client_id: legacy_operator_id.into(),
            token: legacy_operator_token.into(),
        })
        .await
        .unwrap_err();
    assert_eq!(error.code, "LOCAL_OPERATOR_MISMATCH");
    let stale_principal = Principal {
        link_id: "legacy-operator-link".into(),
        client_id: legacy_operator_id.into(),
        role: Role::Operator,
    };
    let error = owner
        .store
        .run(move |db| current_principal(db, stale_principal))
        .await
        .unwrap_err();
    assert_eq!(error.code, "LOCAL_OPERATOR_MISMATCH");

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn legacy_database_pins_custom_bootstrap_identity_only_after_credential_validation() {
    let directory = std::env::temp_dir().join(format!("swarm-local-operator-{}", model::new_id()));
    std::fs::create_dir(&directory).unwrap();
    let root = DataRoot::acquire(&directory).unwrap();
    let custom_credential = Credential {
        client_id: "custom-bootstrap-client".into(),
        token: format!("{}{}", model::new_id(), model::new_id()),
    };
    std::fs::write(
        root.path.join("operator.json"),
        serde_json::to_vec(&custom_credential).unwrap(),
    )
    .unwrap();
    let credential = crate::platform::bootstrap_credential(&root.path).unwrap();
    assert_eq!(credential.client_id, custom_credential.client_id);

    let mut config = Config::default();
    config.storage.data_dir = root.path.clone();
    let owner = StoreOwner::start(root, Arc::new(config), credential.clone())
        .await
        .unwrap();
    let pinned: Value = owner
        .store
        .run(|db| Ok(meta(db, LOCAL_OPERATOR_CLIENT_ID_KEY)?.unwrap()))
        .await
        .unwrap();
    assert_eq!(pinned, json!(credential.client_id));

    // Simulate a pre-pin database. A bad credential must not claim the empty
    // metadata key; the valid file credential pins it on the next open.
    owner
        .store
        .run(|db| {
            db.execute(
                "DELETE FROM meta WHERE key=?1",
                [LOCAL_OPERATOR_CLIENT_ID_KEY],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let wrong_credential = Credential {
        client_id: credential.client_id.clone(),
        token: "wrong-token-for-legacy-upgrade".into(),
    };
    let module_supervisor_credential = owner.module_supervisor_credential();
    owner.close().await.unwrap();
    let mut reopen_config = Config::default();
    reopen_config.storage.data_dir = directory.clone();
    let error = open_database(
        &directory,
        &wrong_credential,
        &module_supervisor_credential,
        &reopen_config,
        None,
        None,
        None,
    )
    .err()
    .unwrap();
    assert_eq!(error.code, "UNAUTHORIZED");
    let db = Connection::open(directory.join("swarm.db")).unwrap();
    assert!(meta(&db, LOCAL_OPERATOR_CLIENT_ID_KEY).unwrap().is_none());
    drop(db);

    let root = DataRoot::acquire(&directory).unwrap();
    let mut config = Config::default();
    config.storage.data_dir = root.path.clone();
    let owner = StoreOwner::start(root, Arc::new(config), credential.clone())
        .await
        .unwrap();
    let pinned: Value = owner
        .store
        .run(|db| Ok(meta(db, LOCAL_OPERATOR_CLIENT_ID_KEY)?.unwrap()))
        .await
        .unwrap();
    assert_eq!(pinned, json!(credential.client_id));
    owner.close().await.unwrap();

    let db = Connection::open(directory.join("swarm.db")).unwrap();
    set_meta(
        &db,
        LOCAL_OPERATOR_CLIENT_ID_KEY,
        &json!("different-client"),
    )
    .unwrap();
    drop(db);
    let error = open_database(
        &directory,
        &credential,
        &module_supervisor_credential,
        &reopen_config,
        None,
        None,
        None,
    )
    .err()
    .unwrap();
    assert_eq!(error.code, "LOCAL_OPERATOR_MISMATCH");
    let db = Connection::open(directory.join("swarm.db")).unwrap();
    assert_eq!(
        meta(&db, LOCAL_OPERATOR_CLIENT_ID_KEY).unwrap(),
        Some(json!("different-client"))
    );
    drop(db);

    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn queued_module_command_from_historical_extra_operator_is_rejected() {
    let mut db = Connection::open_in_memory().unwrap();
    db.execute_batch(SCHEMA).unwrap();
    let now = model::now_ms().unwrap();
    set_meta(
        &db,
        LOCAL_OPERATOR_CLIENT_ID_KEY,
        &json!("bootstrap-operator"),
    )
    .unwrap();
    set_meta(
        &db,
        "client:historical-extra-operator",
        &json!({"role":"operator","token_hash":model::digest(b"retired-token"),"disabled":false}),
    )
    .unwrap();
    set_meta(
        &db,
        "client:fixture-module",
        &json!({"role":"module","binding_id":"binding-1","binding_generation":1,"disabled":false}),
    )
    .unwrap();
    db.execute(
        "INSERT INTO bindings(binding_id,generation,lane_id,module_instance_id,module_artifact_id,state,route_json,state_json,created_at_ms) VALUES('binding-1',1,'lane-1','module-instance-1','artifact-1','ready','{}',?1,?2)",
        rusqlite::params![model::canonical(&json!({"module_client_id":"fixture-module","module_link_id":"fixture-link","connection":"connected"})).unwrap(),now],
    )
    .unwrap();
    db.execute(
        "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,binding_id,binding_generation,state,due_at_ms,created_at_ms,updated_at_ms) VALUES('legacy-op-send','historical-extra-operator','legacy-request','agent.send','{\"delivery\":\"next_turn\"}','{}','binding-1',1,'queued',?1,?2,?2)",
        rusqlite::params![now - 1, now],
    )
    .unwrap();

    let module = Principal {
        link_id: "fixture-link".into(),
        client_id: "fixture-module".into(),
        role: Role::Module,
    };
    let next = runtime::next(&mut db, &module).unwrap();
    assert!(next["command"].is_null());
    assert_eq!(next["rejected_operation_id"], "legacy-op-send");
    assert_eq!(next["error"]["code"], "LOCAL_OPERATOR_MISMATCH");
    let operation = operations::get_operation(&db, "legacy-op-send").unwrap();
    assert_eq!(operation["state"], "rejected");
    assert_eq!(operation["result"]["code"], "LOCAL_OPERATOR_MISMATCH");
}

#[tokio::test]
async fn stale_forge_epoch_cancellation_requires_operator_and_retains_the_diagnosis() {
    let directory = std::env::temp_dir().join(format!("swarm-forge-auth-{}", model::new_id()));
    std::fs::create_dir(&directory).unwrap();
    let root = DataRoot::acquire(&directory).unwrap();
    let credential = crate::platform::bootstrap_credential(&root.path).unwrap();
    let mut config = Config::default();
    config.storage.data_dir = root.path.clone();
    let owner = StoreOwner::start(root, Arc::new(config), credential.clone())
        .await
        .unwrap();
    owner.store.run(|db| {
        let now = model::now_ms()?;
        // The same client holds GM again, but this publication belongs to an
        // earlier epoch. Client identity alone must not restore its authority.
        set_meta(db, "gm", &json!({"client_id":"gm-returned","epoch":3}))?;
        for (id, state, result) in [
            ("forge-stale-queued", "queued", Value::Null),
            ("forge-stale-settled", "settled", json!({"outcome":"stale_gm_epoch","admitted_gm_epoch":1,"current_gm_epoch":3,"publication":"not_started"})),
            ("forge-stale-api", "settled", json!({"outcome":"stale_gm_epoch","admitted_gm_epoch":1,"current_gm_epoch":3,"publication":"not_started"})),
        ] {
            db.execute(
                "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,state,result_json,due_at_ms,settled_at_ms,created_at_ms,updated_at_ms) VALUES(?1,'gm-returned',?1,'forge.publish_ref','{}',?2,?3,?4,?5,?6,?5,?5)",
                rusqlite::params![id,model::canonical(&json!({"publication_intent":{"admitted_gm_epoch":1}}))?,state,if result.is_null(){None}else{Some(model::canonical(&result)?)},now,if state=="settled"{Some(now)}else{None}],
            )?;
        }
        let former = model::Principal { link_id:"test-manager".into(), client_id:"gm-returned".into(), role:Role::Manager };
        let operator = model::Principal { link_id:"test-operator".into(), client_id:"local-operator".into(), role:Role::Operator };
        let tx = db.transaction()?;
        for target in ["forge-stale-queued", "forge-stale-settled"] {
            let params = json!({"client_request_id":model::new_id(),"operation_id":target,"reason":"explicit retirement of old epoch"});
            let error = operations::cancel(&tx, &former, &params, "manager-cancel", now).unwrap_err();
            assert_eq!(error.code, "FORBIDDEN");
            operations::cancel(&tx, &operator, &params, "operator-cancel", now)?;
            assert_eq!(operations::get_operation(&tx, target)?["state"], "cancelled");
        }
        let settled = operations::get_operation(&tx, "forge-stale-settled")?;
        assert_eq!(settled["result"]["previous_result"]["outcome"], "stale_gm_epoch");
        assert_eq!(settled["result"]["previous_result"]["admitted_gm_epoch"], 1);
        tx.commit()?;
        Ok(())
    }).await.unwrap();

    let canonical_operator = owner.store.authenticate(credential).await.unwrap();
    assert_eq!(canonical_operator.role, Role::Operator);
    let receipt = owner
        .store
        .call(
            canonical_operator,
            "operation.cancel".into(),
            json!({"client_request_id":model::new_id(),"operation_id":"forge-stale-api",
                   "reason":"local operator retires stale publication"}),
        )
        .await
        .unwrap();
    assert_eq!(receipt["cancelled_operation_id"], "forge-stale-api");
    let cancelled = owner
        .store
        .run(|db| operations::get_operation(db, "forge-stale-api"))
        .await
        .unwrap();
    assert_eq!(cancelled["state"], "cancelled");
    assert_eq!(
        cancelled["result"]["previous_result"]["outcome"],
        "stale_gm_epoch"
    );
    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}
