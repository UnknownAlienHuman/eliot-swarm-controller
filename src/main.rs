use std::path::PathBuf;
use clap::{Parser,Subcommand};
use serde_json::{Value,json};
use eliot_swarm_controller::{config::Config,error::{Error,Result},host,ipc,model::{self,Credential},platform};

#[derive(Parser)]
#[command(name="swarm",version,about="Headless local task controller. Native adapters are not connected in this build.")]
struct Cli {
    #[arg(long,global=true)] config:Option<PathBuf>,
    #[arg(long,global=true)] data_dir:Option<PathBuf>,
    #[arg(long,global=true)] credential:Option<PathBuf>,
    #[arg(long,global=true)] request_id:Option<String>,
    #[command(subcommand)] command:Command,
}
#[derive(Subcommand)]
enum Command {
    /// Start the user host in the foreground; never launches vendor agents implicitly.
    Host,
    Status,
    /// Call a supported application method; JSON params are read from a file.
    Call {method:String,#[arg(long)] file:Option<PathBuf>},
    Task {#[command(subcommand)] command:TaskCommand},
    /// Create a scoped local client credential. Run as the local operator.
    ClientCreate {client_id:String,#[arg(long,default_value="manager",value_parser=["manager","observer"])] role:String,#[arg(long)] out:PathBuf},
    Report {#[arg(long,default_value_t=0)] after:i64,#[arg(long,default_value_t=50)] limit:i64},
}
#[derive(Subcommand)]
enum TaskCommand {
    Create {#[arg(long)] project:String,#[arg(long)] file:PathBuf,#[arg(long)] origin_key:Option<String>},
    Get {task_id:String},
    List {#[arg(long,default_value_t=0)] after:i64,#[arg(long,default_value_t=50)] limit:i64},
    Claim {task_id:String,#[arg(long)] revision:i64,#[arg(long)] owner:Option<String>,#[arg(long,default_value="native_manager",value_parser=["controller","native_manager"])] start_owner:String},
    Revise {task_id:String,#[arg(long)] revision:i64,#[arg(long)] file:PathBuf},
}
fn read_json(file:&PathBuf)->Result<Value>{Ok(serde_json::from_slice(&std::fs::read(file)?)?)}
#[tokio::main]
async fn main(){
    if let Err(e)=run(Cli::parse()).await{
        eprintln!("{}",json!({"error":e}));std::process::exit(1);
    }
}
async fn run(cli:Cli)->Result<()> {
    let config=Config::load(cli.config.as_deref(),cli.data_dir.as_deref())?;
    if matches!(&cli.command,Command::Host){return host::run(config).await;}
    let credential=platform::load_credential(&cli.credential.unwrap_or_else(||config.storage.data_dir.join("operator.json")))?;
    let mut pending_credential=None;
    let(method,mut params)=match cli.command{
        Command::Host=>unreachable!("host returned above"),
        Command::Status=>("host.status".to_string(),json!({})),
        Command::Call{method,file}=>(method,if let Some(p)=file{read_json(&p)?}else{json!({})}),
        Command::Report{after,limit}=>("report.delta".into(),json!({"after":after,"limit":limit})),
        Command::Task{command}=>match command{
            TaskCommand::Create{project,file,origin_key}=>{
                let mut value=json!({"project_id":project,"spec":read_json(&file)?});
                if let Some(origin)=origin_key{value["origin_key"]=json!(origin);}
                ("task.create".into(),value)
            }
            TaskCommand::Get{task_id}=>("task.get".into(),json!({"task_id":task_id})),
            TaskCommand::List{after,limit}=>("task.list".into(),json!({"after":after,"limit":limit})),
            TaskCommand::Claim{task_id,revision,owner,start_owner}=>{
                let mut value=json!({"task_id":task_id,"expected_revision":revision,"start_owner":start_owner});
                if let Some(owner)=owner{value["owner_id"]=json!(owner);}
                ("task.claim".into(),value)
            }
            TaskCommand::Revise{task_id,revision,file}=>("task.revise".into(),json!({"task_id":task_id,"expected_revision":revision,"spec":read_json(&file)?})),
        }
        Command::ClientCreate{client_id,role,out}=>{
            // Save the secret first. A lost registration reply cannot strand its owner.
            // Existing files are re-used, never silently rotated on a retry.
            let new=if out.try_exists()?{
                let c=platform::load_credential(&out)?;
                if c.client_id!=client_id{return Err(Error::invalid("credential file belongs to a different client"));}c
            }else{
                let c=Credential{client_id:client_id.clone(),token:format!("{}{}",model::new_id(),model::new_id())};
                platform::write_private_new(&out,&serde_json::to_vec_pretty(&c)?)?;c
            };
            let value=json!({"client_id":client_id,"role":role,"token_hash":model::digest(new.token.as_bytes())});
            pending_credential=Some(out);
            ("client.register".into(),value)
        }
    };
    let is_read=matches!(method.as_str(),"host.status"|"task.get"|"task.list"|"attempt.get"|"operation.get"|"operation.list"|"agent.state"|"agent.list"|"route.list"|"report.delta"|"message.read"|"client.list");
    if !is_read {
        if !params.is_object(){return Err(Error::invalid("params file must contain an object"));}
        if let Some(id)=cli.request_id{params["client_request_id"]=json!(id);}
        else if params.get("client_request_id").is_none(){params["client_request_id"]=json!(model::new_id());}
        eprintln!("client_request_id={}",model::text(&params,"client_request_id")?);
    }
    let result=ipc::call(&config.storage.data_dir,&credential,&method,params,&config.ipc).await?;
    println!("{}",serde_json::to_string_pretty(&result)?);
    if let Some(path)=pending_credential{eprintln!("credential saved: {}",path.display());}
    Ok(())
}
