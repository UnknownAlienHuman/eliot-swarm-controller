use crate::{
    config::Config,
    error::Result,
    ipc,
    platform::{DataRoot, bootstrap_credential},
    store::StoreOwner,
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
    let checks = tokio::spawn(owner.store.clone().supervise_checks(stopping.clone()));
    let opencode = tokio::spawn(owner.store.clone().supervise_opencode(stopping.clone()));
    let semaphore = Arc::new(Semaphore::new(config.ipc.max_connections));
    let mut connections = JoinSet::new();
    eprintln!("swarm host ready: {}", listener.endpoint());
    let exit = loop {
        tokio::select! {
            signal=tokio::signal::ctrl_c()=>break signal.map_err(Into::into),
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
    let _ = checks.await;
    let _ = opencode.await;
    owner.close().await?;
    exit
}
