use super::*;
use crate::{
    bus::Bus,
    plugin_host::host::PluginHostOptions,
    test_support::windows_family::{Family, Owner},
};
use std::{
    collections::{HashMap, HashSet},
    process::Stdio,
    sync::Mutex,
    time::Duration,
};

fn host() -> PluginHost {
    let store = Arc::new(tokio::sync::Mutex::new(
        eliot_store::Store::open(std::path::Path::new(":memory:")).unwrap(),
    ));
    let bus = Bus::new(64);
    let secrets = Arc::new(Mutex::new(HashSet::new()));
    let monitoring = Monitoring::new(store.clone(), bus.clone(), secrets.clone());
    let (progress, _) = tokio::sync::watch::channel(0);
    PluginHost::new(
        PluginHostOptions::new(PathBuf::new(), "owner-test".into()),
        store,
        bus,
        secrets,
        Arc::new(Mutex::new(HashMap::new())),
        Arc::new(Mutex::new(HashSet::new())),
        progress,
        monitoring,
    )
}

fn reader(family: &Family, mode: &str) -> ReaderTransport {
    let mut command = family.command(mode);
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = OwnedChild::spawn(command).unwrap();
    let observer = child.observer().ok();
    let stderr = spawn_stderr(
        child.take_stderr().unwrap(),
        Arc::new(SecretPolicy::default()),
    );
    let mut connection =
        PluginConnection::new(child.take_stdout().unwrap(), child.take_stdin().unwrap());
    let client = connection.client();
    let requests = tokio::spawn(async move { while connection.next_request().await.is_some() {} });
    ReaderTransport {
        child,
        client,
        secrets: Arc::from([]),
        metadata_secrets: Arc::from([]),
        credential: "reader-test".into(),
        instance_dir: family.root.join("instance"),
        store_root: family.root.clone(),
        observer,
        requests,
        stderr,
    }
}

#[tokio::test]
async fn catalogue_reader_closes_grandchild_after_root_and_stdout_exit() {
    let family = Family::new();
    let reader = reader(&family, "read");
    let identities = family.ready().await;
    family.root_exited(identities[0]).await;
    assert!(reader.close(&host(), None).await.closed);
    family.assert_closed(&identities).await;
}

#[tokio::test]
async fn catalogue_reader_drain_closes_grandchild_family() {
    let family = Family::new();
    let reader = reader(&family, "reader");
    let identities = family.ready().await;
    assert!(reader.close(&host(), None).await.closed);
    family.assert_closed(&identities).await;
}

#[tokio::test]
async fn catalogue_reader_expired_cleanup_never_confirms_closure() {
    let family = Family::new();
    let reader = reader(&family, "hold");
    let identities = family.ready().await;
    assert!(
        !reader
            .close_until(&host(), None, Instant::now())
            .await
            .closed
    );
    family.assert_closed(&identities).await;
}

#[tokio::test]
async fn catalogue_reader_owner_hard_kill_closes_grandchild_family() {
    let family = Family::new();
    let mut owner = Owner(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "plugin_host::host::reader::family_tests::reader_owner",
                "--nocapture",
            ])
            .env("ELIOT_READER_FAMILY_ROOT", &family.root)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let identities = family.ready().await;
    owner.0.kill().unwrap();
    owner.0.wait().unwrap();
    family.assert_closed(&identities).await;
}

#[tokio::test]
async fn reader_owner() {
    let Some(root) = std::env::var_os("ELIOT_READER_FAMILY_ROOT") else {
        return;
    };
    let family = Family::at(PathBuf::from(root));
    let _reader = reader(&family, "hold");
    tokio::time::sleep(Duration::from_secs(60)).await;
}
