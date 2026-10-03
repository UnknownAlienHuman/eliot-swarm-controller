use crate::{
    config::Config,
    error::{Error, Result},
    ipc,
    platform::{DataRoot, bootstrap_credential},
    store::{Store, StoreOwner},
};
use std::sync::Arc;
use tokio::{
    sync::{Semaphore, watch},
    task::JoinSet,
};

pub async fn run(config: Config) -> Result<()> {
    let root = DataRoot::acquire(&config.storage.data_dir)?;
    let credential = bootstrap_credential(&root.path)?;
    let root_path = root.path.clone();
    let config = Arc::new(config);
    let owner = StoreOwner::start(root, config.clone(), credential).await?;
    let ipc_config = Arc::new(config.ipc.clone());
    let mut listener = ipc::Listener::bind(&root_path)?;
    let (shutdown, stopping) = watch::channel(false);
    let mut supervisors: JoinSet<(&'static str, Result<()>)> = JoinSet::new();
    let store = owner.store.clone();
    let stop = stopping.clone();
    supervisors.spawn(async move {
        store.supervise_checks(stop).await;
        ("checks", Ok(()))
    });
    let store = owner.store.clone();
    let stop = stopping.clone();
    supervisors.spawn(async move {
        store.supervise_opencode(stop).await;
        ("opencode", Ok(()))
    });
    let store = owner.store.clone();
    let stop = stopping.clone();
    supervisors.spawn(async move {
        store.supervise_zed(stop).await;
        ("zed", Ok(()))
    });
    let store = owner.store.clone();
    let stop = stopping.clone();
    supervisors.spawn(async move { ("scheduler", crate::scheduler::run(store, stop).await) });
    let store = owner.store.clone();
    let stop = stopping.clone();
    supervisors.spawn(async move { ("automation", supervise_automation(store, stop).await) });
    let store = owner.store.clone();
    let stop = stopping.clone();
    supervisors.spawn(async move { ("forge", supervise_forge(store, stop).await) });
    let semaphore = Arc::new(Semaphore::new(config.ipc.max_connections));
    let mut connections = JoinSet::new();
    eprintln!("swarm host ready: {}", listener.endpoint());
    let exit = loop {
        tokio::select! {
            signal=tokio::signal::ctrl_c()=>break signal.map_err(Into::into),
            Some(result)=supervisors.join_next()=>{
                break match result {
                    Ok((name, Ok(()))) => Err(Error::new("SUPERVISOR_STOPPED", format!("{name} stopped before host shutdown"))),
                    Ok((name, Err(error))) => Err(Error::new(error.code, format!("{name}: {}", error.message))),
                    Err(error) => Err(Error::new("SUPERVISOR_FAILED", error.to_string())),
                };
            }
            Some(result)=connections.join_next(),if !connections.is_empty()=>{
                if let Err(e)=result{eprintln!("IPC worker ended: {e}");}
            }
            accepted=listener.accept()=>{
                let stream=match accepted{Ok(s)=>s,Err(e)=>break Err(e)};
                let permit=match semaphore.clone().try_acquire_owned(){Ok(p)=>p,Err(_)=>{drop(stream);continue}};
                let store=owner.store.clone();let config=ipc_config.clone();let stopping=stopping.clone();
                connections.spawn(async move{
                    let _permit=permit;
                    if let Err(e)=ipc::serve(stream,store,config,stopping).await{eprintln!("IPC connection: {}",e.code);}
                });
            }
        }
    };
    drop(listener);
    let _ = shutdown.send(true);
    while connections.join_next().await.is_some() {}
    // Await host-owned workers. Dropping the IPC caller or beginning shutdown
    // must not detach or replay an already admitted external publication.
    while let Some(result) = supervisors.join_next().await {
        if let Ok((name, Err(error))) = result {
            eprintln!("{name} supervisor: {}", error.code);
        } else if let Err(error) = result {
            eprintln!("supervisor join failed: {error}");
        }
    }
    owner.close().await?;
    exit
}

/// One host-owned reconciler for all enabled entries. Committed changes are
/// hints; bounded startup/periodic scans recover after missed hints or restart.
async fn supervise_automation(store: Store, mut stopping: watch::Receiver<bool>) -> Result<()> {
    let mut changed = store.subscribe_schedule_changes();
    let mut tick = tokio::time::interval(std::time::Duration::from_secs(2));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        if *stopping.borrow() {
            return Ok(());
        }
        tokio::select! {
            result = stopping.changed() => {
                if result.is_err() || *stopping.borrow() { return Ok(()); }
            }
            result = changed.changed() => {
                if result.is_err() { return Err(Error::new("STORE_CLOSED", "automation change stream ended")); }
                if *stopping.borrow() { return Ok(()); }
                store.reconcile_automations_once().await?;
            }
            _ = tick.tick() => {
                if *stopping.borrow() { return Ok(()); }
                store.reconcile_automations_once().await?;
            }
        }
    }
}

async fn supervise_forge(store: Store, mut stopping: watch::Receiver<bool>) -> Result<()> {
    let mut tick = tokio::time::interval(std::time::Duration::from_secs(5));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        if *stopping.borrow() {
            return Ok(());
        }
        tokio::select! {
            result = stopping.changed() => {
                if result.is_err() || *stopping.borrow() { return Ok(()); }
            }
            _ = tick.tick() => {
                if *stopping.borrow() { return Ok(()); }
                // This pass is awaited through shutdown, independent of IPC.
                // The Store reads unknown outcomes before never-sent effects.
                store.supervise_forge_once().await?;
            }
        }
    }
}
