use super::{Config, Error, Principal, Result, Role};
use rusqlite::{Connection, TransactionBehavior};
use serde_json::Value;
use std::{path::Path, path::PathBuf, sync::Arc, thread::JoinHandle};
use tokio::sync::{mpsc, oneshot};

struct StatusRequest {
    principal: Principal,
    method: String,
    params: Value,
    reply: oneshot::Sender<Result<Value>>,
}

enum StatusJob {
    Read(StatusRequest),
    Stop,
}

#[derive(Clone)]
pub(super) struct Sender(mpsc::Sender<StatusJob>);

impl Sender {
    pub(super) async fn host_status(&self, principal: Principal, params: Value) -> Result<Value> {
        self.read(principal, "host.status", params).await
    }

    pub(super) async fn monitor(
        &self,
        principal: Principal,
        method: String,
        params: Value,
    ) -> Result<Value> {
        self.read(principal, &method, params).await
    }

    async fn read(&self, principal: Principal, method: &str, params: Value) -> Result<Value> {
        let (reply, response) = oneshot::channel();
        self.0
            .send(StatusJob::Read(StatusRequest {
                principal,
                method: method.to_owned(),
                params,
                reply,
            }))
            .await
            .map_err(|_| Error::new("STORE_CLOSED", "status reader stopped"))?;
        response
            .await
            .map_err(|_| Error::new("STORE_CLOSED", "status read lost its response"))?
    }

    /// Close the existing reader FIFO even while Store clones retain senders.
    /// Requests before this marker finish normally; later queued replies are
    /// dropped when the receiver exits and report STORE_CLOSED to their callers.
    pub(super) async fn shutdown(&self) -> Result<()> {
        self.0
            .send(StatusJob::Stop)
            .await
            .map_err(|_| Error::new("STORE_CLOSED", "status reader stopped before shutdown"))
    }
}

pub(super) async fn start(
    database_path: PathBuf,
    queue_capacity: usize,
    config: Arc<Config>,
) -> Result<(Sender, JoinHandle<()>)> {
    if queue_capacity == 0 {
        return Err(Error::new(
            "STORE_CONFIGURATION",
            "status reader queue capacity must be positive",
        ));
    }

    let (tx, mut rx) = mpsc::channel::<StatusJob>(queue_capacity);
    let (ready_tx, ready_rx) = oneshot::channel();
    let thread = std::thread::Builder::new()
        .name("swarm-status-reader".into())
        .spawn(move || {
            let mut db = match open_status_database(&database_path) {
                Ok(db) => db,
                Err(error) => {
                    let _ = ready_tx.send(Err(error));
                    return;
                }
            };
            if ready_tx.send(Ok(())).is_err() {
                return;
            }
            while let Some(job) = rx.blocking_recv() {
                let StatusJob::Read(request) = job else {
                    break;
                };
                let StatusRequest {
                    principal,
                    method,
                    params,
                    reply,
                } = request;
                let result = read_status(&mut db, principal, &method, params, &config);
                let _ = reply.send(result);
            }
        })?;

    match ready_rx.await {
        Ok(Ok(())) => Ok((Sender(tx), thread)),
        Ok(Err(error)) => {
            join_status_thread(thread).await?;
            Err(error)
        }
        Err(_) => {
            join_status_thread(thread).await?;
            Err(Error::new(
                "STORE_CLOSED",
                "status reader initialization thread ended",
            ))
        }
    }
}

async fn join_status_thread(thread: JoinHandle<()>) -> Result<()> {
    tokio::task::spawn_blocking(move || thread.join())
        .await
        .map_err(|error| {
            Error::new(
                "STORE_CLOSED",
                format!("status reader join failed: {error}"),
            )
        })?
        .map_err(|_| Error::new("STORE_PANIC", "status reader panicked during startup"))
}

fn open_status_database(path: &Path) -> Result<Connection> {
    swarm_store::open_reader(
        path,
        swarm_store::SchemaIdentity {
            application_id: super::APPLICATION_ID,
            user_version: 1,
            base_schema: super::SCHEMA,
        },
        swarm_store::ReaderOptions::default(),
    )
    .map_err(Into::into)
}

fn read_status(
    db: &mut Connection,
    principal: Principal,
    method: &str,
    params: Value,
    config: &Config,
) -> Result<Value> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Deferred)?;
    let result = (|| {
        let principal = super::current_principal(&tx, principal)?;
        if principal.role == Role::Module {
            return Err(Error::new(
                "FORBIDDEN",
                "module credentials serve only their native binding",
            ));
        }
        if principal.role == Role::Participant {
            return Err(Error::new(
                "FORBIDDEN",
                "participant credentials have no host-wide status surface",
            ));
        }
        super::read(&tx, &principal, method, &params, config)
    })();

    match result {
        Ok(status) => {
            tx.commit()?;
            Ok(status)
        }
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::Config,
        model::{Credential, Role, new_id},
        platform::DataRoot,
    };
    use rusqlite::ErrorCode;
    use serde_json::json;
    use std::{
        fs,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
            mpsc as std_mpsc,
        },
        time::Duration,
    };

    struct ScratchDir(PathBuf);

    impl ScratchDir {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!("eliot-status-reader-{}", new_id()));
            fs::create_dir(&path).expect("create unique test directory");
            Self(path)
        }
    }

    impl Drop for ScratchDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    struct Fixture {
        directory: ScratchDir,
        owner: super::super::StoreOwner,
        principal: Principal,
    }

    impl Fixture {
        async fn new() -> Self {
            let directory = ScratchDir::new();
            let root = DataRoot::acquire(&directory.0).expect("acquire test data root");
            let credential = Credential {
                client_id: "operator".into(),
                token: format!("status-reader-test-{}", new_id()),
            };
            let owner = super::super::StoreOwner::start(
                root,
                Arc::new(Config::default()),
                credential.clone(),
            )
            .await
            .expect("start fixture store and status reader");
            let principal = owner
                .store
                .authenticate(credential)
                .await
                .expect("authenticate fixture operator");
            Self {
                directory,
                owner,
                principal,
            }
        }

        async fn status(&self) -> Result<Value> {
            self.owner
                .store
                .call(self.principal.clone(), "host.status".into(), json!({}))
                .await
        }

        async fn close(self) {
            self.owner.close().await.expect("close fixture store");
            let _ = &self.directory;
        }
    }

    #[tokio::test]
    async fn missing_database_is_not_created() {
        let directory = ScratchDir::new();
        let path = directory.0.join("swarm.db");
        let error = match start(path.clone(), 4, Arc::new(Config::default())).await {
            Ok(_) => panic!("read-only startup must reject a missing database"),
            Err(error) => error,
        };
        assert_eq!(error.code, "STORE_ERROR");
        assert!(
            !path.exists(),
            "read-only open must not create the database"
        );
    }

    #[tokio::test]
    async fn owner_joins_database_thread_when_initialization_fails() {
        let directory = ScratchDir::new();
        let root = DataRoot::acquire(&directory.0).expect("acquire test data root");
        fs::write(directory.0.join("swarm.db"), b"not a sqlite database")
            .expect("write invalid database fixture");
        let credential = Credential {
            client_id: "operator".into(),
            token: format!("status-reader-test-{}", new_id()),
        };

        let result = tokio::time::timeout(
            Duration::from_secs(2),
            super::super::StoreOwner::start(root, Arc::new(Config::default()), credential),
        )
        .await
        .expect("failed initialization must join rather than strand its DB thread");
        let error = match result {
            Ok(owner) => {
                owner
                    .close()
                    .await
                    .expect("close unexpected successful owner");
                panic!("corrupt database fixture must fail initialization");
            }
            Err(error) => error,
        };
        assert_eq!(error.code, "STORE_ERROR");

        let _reacquired = DataRoot::acquire(&directory.0)
            .expect("failed initialization must release the data-root lock");
    }

    #[tokio::test]
    async fn status_connection_rejects_writes() {
        let fixture = Fixture::new().await;
        let connection = open_status_database(&fixture.directory.0.join("swarm.db"))
            .expect("open initialized database in read-only mode");
        let query_only: i64 = connection
            .pragma_query_value(None, "query_only", |row| row.get(0))
            .expect("read query_only setting");
        assert_eq!(query_only, 1);

        let error = connection
            .execute(
                "UPDATE meta SET value_json='999' WHERE key='host_epoch'",
                [],
            )
            .expect_err("status connection must reject writes");
        assert!(matches!(
            error,
            rusqlite::Error::SqliteFailure(ref details, _)
                if details.code == ErrorCode::ReadOnly
        ));
        assert!(fixture.status().await.is_ok());
        drop(connection);
        fixture.close().await;
    }

    #[tokio::test]
    async fn status_rechecks_revoked_principal() {
        let fixture = Fixture::new().await;
        let store = fixture.owner.store.clone();
        store
            .run(move |db| {
                let tx = db.transaction()?;
                let key = "client:operator";
                let mut registration = super::super::meta(&tx, key)?
                    .ok_or_else(|| Error::new("TEST_FIXTURE", "operator metadata missing"))?;
                registration["disabled"] = json!(true);
                super::super::set_meta(&tx, key, &registration)?;
                tx.commit()?;
                Ok(())
            })
            .await
            .expect("commit principal revocation");
        drop(store);

        let error = fixture
            .status()
            .await
            .expect_err("status must revalidate the current principal");
        assert_eq!(error.code, "UNAUTHORIZED");
        fixture.close().await;
    }

    #[tokio::test]
    async fn status_rejects_module_principal() {
        let fixture = Fixture::new().await;
        let store = fixture.owner.store.clone();
        store
            .run(move |db| {
                let tx = db.transaction()?;
                super::super::set_meta(
                    &tx,
                    "client:module-fixture",
                    &json!({"role":"module","disabled":false}),
                )?;
                tx.commit()?;
                Ok(())
            })
            .await
            .expect("register module principal fixture");
        drop(store);

        let module = Principal {
            link_id: new_id(),
            client_id: "module-fixture".into(),
            role: Role::Module,
        };
        let error = fixture
            .owner
            .store
            .call(module, "host.status".into(), json!({}))
            .await
            .expect_err("module principal must not read host status");
        assert_eq!(error.code, "FORBIDDEN");
        fixture.close().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn status_reads_committed_snapshot_while_writer_is_held() {
        let fixture = Fixture::new().await;
        let before = fixture.status().await.expect("initial status");
        let initial_epoch = before["host_epoch"].clone();

        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = std_mpsc::channel();
        let writer_store = fixture.owner.store.clone();
        let writer = tokio::spawn(async move {
            writer_store
                .run(move |db| {
                    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                    super::super::set_meta(&tx, "host_epoch", &json!(991))?;
                    entered_tx
                        .send(())
                        .map_err(|_| Error::new("TEST_SYNC", "status test stopped waiting"))?;
                    release_rx
                        .recv()
                        .map_err(|error| Error::new("TEST_SYNC", error.to_string()))?;
                    tx.commit()?;
                    Ok(())
                })
                .await
        });
        entered_rx
            .await
            .expect("writer holds its uncommitted transaction");

        let during_write = tokio::time::timeout(Duration::from_secs(2), fixture.status())
            .await
            .expect("status must not wait behind a held WAL writer")
            .expect("status reader returns the committed view");
        assert_eq!(during_write["host_epoch"], initial_epoch);

        release_tx.send(()).expect("release writer transaction");
        writer
            .await
            .expect("writer task joins")
            .expect("writer commit");
        let after_commit = fixture.status().await.expect("read committed status");
        assert_eq!(after_commit["host_epoch"], json!(991));
        fixture.close().await;
    }

    #[tokio::test]
    async fn owner_close_joins_database_even_if_status_reader_panicked() {
        let database_joined = Arc::new(AtomicBool::new(false));
        let status_thread = std::thread::spawn(|| panic!("simulated status-reader panic"));
        let joined = database_joined.clone();
        let database_thread = std::thread::spawn(move || {
            joined.store(true, Ordering::SeqCst);
        });

        let error = super::super::join_store_threads(status_thread, database_thread)
            .await
            .expect_err("the status-reader panic is reported");
        assert_eq!(error.code, "STORE_PANIC");
        assert!(
            database_joined.load(Ordering::SeqCst),
            "database owner must be joined even after status-reader panic"
        );
    }
}
