use super::{native_read, probe_muse_command};
use crate::test_support::windows_family::{Family, Owner};
use std::{path::PathBuf, process::Stdio, time::Duration};

fn start_read(family: &Family) -> tokio::task::JoinHandle<Option<Vec<u8>>> {
    let command = family.command("read");
    let program = command
        .as_std()
        .get_program()
        .to_string_lossy()
        .into_owned();
    let args: Vec<_> = command
        .as_std()
        .get_args()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect();
    tokio::task::spawn_blocking(move || {
        let args: Vec<_> = args.iter().map(String::as_str).collect();
        native_read(&program, &args, Duration::from_secs(30))
    })
}

#[tokio::test]
async fn native_read_waits_for_grandchild_after_root_and_stdout_exit() {
    let family = Family::new();
    let read = start_read(&family);
    let identities = family.ready().await;
    family.root_exited(identities[0]).await;
    assert!(
        !read.is_finished(),
        "root exit/EOF cannot prove family closure"
    );
    family.release();
    let output = read.await.unwrap().expect("confirmed complete read");
    assert!(output.starts_with(b"catalogue"));
    family.assert_closed(&identities).await;
}

#[tokio::test]
async fn native_read_timeout_reaps_grandchild_after_root_exit() {
    let family = Family::new();
    let read = start_read(&family);
    let identities = family.ready().await;
    family.root_exited(identities[0]).await;
    assert!(
        read.await.unwrap().is_none(),
        "interrupted family is never successful output"
    );
    family.assert_closed(&identities).await;
}

#[tokio::test]
async fn probe_read_and_drain_close_grandchild_family() {
    for mode in ["probe", "hold"] {
        let family = Family::new();
        let read = tokio::spawn(probe_muse_command(
            family.command(mode),
            Duration::from_secs(30),
        ));
        let identities = family.ready().await;
        assert_eq!(read.await.unwrap(), mode == "probe");
        family.assert_closed(&identities).await;
    }
}

#[tokio::test]
async fn probe_cancellation_closes_grandchild_family() {
    let family = Family::new();
    let read = tokio::spawn(probe_muse_command(
        family.command("hold"),
        Duration::from_secs(30),
    ));
    let identities = family.ready().await;
    read.abort();
    assert!(read.await.unwrap_err().is_cancelled());
    family.assert_closed(&identities).await;
}

#[tokio::test]
async fn native_and_probe_owner_hard_kill_close_grandchild_families() {
    for mode in ["native", "probe"] {
        let family = Family::new();
        let mut owner = Owner(
            std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "adhoc::family_tests::bridge_owner",
                    "--nocapture",
                ])
                .env("ELIOT_FAMILY_OWNER", mode)
                .env("ELIOT_FAMILY_ROOT", &family.root)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        );
        let identities = family.ready().await;
        owner.0.kill().unwrap(); // only this test's exact process handle
        owner.0.wait().unwrap();
        family.assert_closed(&identities).await;
    }
}

#[tokio::test]
async fn bridge_owner() {
    let Some(mode) = std::env::var_os("ELIOT_FAMILY_OWNER") else {
        return;
    };
    let family = Family::at(PathBuf::from(
        std::env::var_os("ELIOT_FAMILY_ROOT").unwrap(),
    ));
    if mode == "native" {
        let _ = start_read(&family).await;
    } else {
        let _ = probe_muse_command(family.command("hold"), Duration::from_secs(30)).await;
    }
}
