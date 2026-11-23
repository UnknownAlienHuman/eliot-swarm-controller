//! Thin dispatch for the `swarm` CLI.
//! Each command family owns its `CliCommand` arms outright; this
//! module only routes a parsed command to the owning family runner.
mod agents;
pub(crate) mod args;
mod autonomy;
mod autostart;
mod cleanup;
mod coord;
mod daemon;
mod git;
mod operator;
mod organization;
mod project_assignment;
mod project_broadcast;
mod reads;
mod rules;
mod settings;
mod stand;
mod store;
pub(crate) mod support;
mod update;

use self::args::{CliCommand, DaemonAction, LsArgs, PageArgs, StandAction};
use self::support::Failure;
pub(crate) use reads::report_doctor_acl;
use std::path::{Path, PathBuf};

pub(crate) fn read_only_stand(
    command: CliCommand,
    data_dir: &Path,
    json_output: bool,
    id: Option<String>,
) -> Result<u8, Failure> {
    if id.is_some() {
        return Err(Failure::new(
            2,
            "--id is unavailable on a read-only Store stand",
        ));
    }
    match command {
        CliCommand::Status => {
            let high_water = eliot_daemon::operator::stand_copy_event_high_water(data_dir)
                .map_err(Failure::for_read)?;
            support::print_value_or_human(
                &serde_json::json!({"mode":"read_only", "event_high_water":high_water}),
                json_output,
                || {
                    println!("Read-only Store snapshot · event high-water {high_water}");
                },
            );
        }
        CliCommand::Ls(LsArgs {
            after,
            page: PageArgs { limit },
            filter,
        }) => {
            let request = reads::list_params(after, limit, false, json_output, filter)?;
            let result = eliot_daemon::operator::stand_copy_agents(data_dir, &request)
                .map_err(Failure::for_read)?;
            if json_output {
                let value = serde_json::to_value(result)
                    .map_err(|_| Failure::new(4, "snapshot Agent list could not be encoded"))?;
                support::print_value(&value, true);
            } else {
                reads::print_agent_table(&result, false, 0, false);
            }
        }
        _ => {
            return Err(Failure::new(
                3,
                "read-only Store stands support status and ls",
            ));
        }
    }
    Ok(0)
}

pub(crate) async fn dispatch(
    command: CliCommand,
    data_dir: PathBuf,
    json_output: bool,
    id: Option<String>,
    doctor_acl_report: Option<eliot_core::Result<Vec<eliot_daemon::ForeignDataDirAce>>>,
) -> Result<u8, Failure> {
    match command {
        CliCommand::Stand(args) => match args.action {
            StandAction::Up {
                root,
                settings,
                profile,
                read_only_copy,
                binary,
                ttl,
            } => {
                Box::pin(stand::up(
                    root,
                    settings,
                    profile,
                    read_only_copy,
                    binary,
                    ttl,
                    json_output,
                ))
                .await
            }
            StandAction::Down { path } => stand::down(path, json_output).await,
            StandAction::List => stand::list(json_output).await,
            StandAction::Prune => stand::prune(json_output).await,
        },
        CliCommand::Cleanup(args) => cleanup::run_cleanup(args, data_dir, json_output, id).await,
        CliCommand::Artifact(args) => cleanup::run_artifact(args, data_dir, json_output, id).await,
        CliCommand::Settings(args) => settings::run(args, data_dir, json_output).await,
        CliCommand::Daemon(args) if args.interrupt_on_deadline => {
            let Some(DaemonAction::Update {
                release,
                wait_seconds,
            }) = args.action
            else {
                return Err(Failure::new(
                    2,
                    "--interrupt-on-deadline requires daemon update",
                ));
            };
            if args.foreground || id.is_some() {
                return Err(Failure::new(
                    2,
                    "--foreground and --id are invalid with daemon update",
                ));
            }
            update::run_interrupting(release, data_dir, wait_seconds, json_output).await
        }
        CliCommand::Daemon(args) => daemon::run_daemon(args, data_dir, json_output, id).await,
        CliCommand::Mcp(args) => daemon::run_mcp(args, data_dir, json_output).await,
        CliCommand::Status => daemon::run_status(data_dir, json_output).await,
        CliCommand::Autonomy(args) => autonomy::run(args, &data_dir, json_output).await,
        CliCommand::Maintenance(args) => daemon::run_maintenance(args, data_dir, json_output).await,
        CliCommand::Store(args) => store::run_store(args, data_dir, json_output),
        CliCommand::Provider(args) => rules::run_provider(args, data_dir, json_output, id).await,
        CliCommand::Project(args) => rules::run_project(args, data_dir, json_output, id).await,
        CliCommand::Rule(args) => rules::run_rule(args, data_dir, json_output, id).await,
        CliCommand::Timer(args) => rules::run_timer(args, data_dir, json_output, id).await,
        CliCommand::Goal(args) => rules::run_goal(args, data_dir, json_output, id).await,
        CliCommand::Discover(args) => agents::run_discover(args, data_dir, json_output).await,
        CliCommand::Harnesses => agents::run_harnesses(data_dir, json_output).await,
        CliCommand::Models(args) => agents::run_models(args, data_dir, json_output).await,
        CliCommand::Quota(args) => agents::run_quota(args, data_dir, json_output).await,
        CliCommand::Attach(args) => agents::run_attach(args, data_dir, json_output, id).await,
        CliCommand::Archive(args) => {
            agents::run_tree_change(args.agent, Some(true), data_dir, json_output, id).await
        }
        CliCommand::Unarchive(args) => {
            agents::run_tree_change(args.agent, Some(false), data_dir, json_output, id).await
        }
        CliCommand::Delete(args) => {
            agents::run_tree_change(args.agent, None, data_dir, json_output, id).await
        }
        CliCommand::Launch(args) => agents::run_launch(args, data_dir, json_output, id).await,
        CliCommand::LaunchQueue(args) => {
            agents::run_launch_queue(args, data_dir, json_output).await
        }
        CliCommand::Send(args) => agents::run_send(args, data_dir, json_output, id).await,
        CliCommand::Resume(args) => agents::run_resume(args, data_dir, json_output, id).await,
        CliCommand::Steer(args) => agents::run_steer(args, data_dir, json_output, id).await,
        CliCommand::Inject(args) => agents::run_inject(args, data_dir, json_output, id).await,
        CliCommand::Interrupt(args) => agents::run_interrupt(args, data_dir, json_output, id).await,
        CliCommand::Approve(args) => agents::run_approve(args, data_dir, json_output, id).await,
        CliCommand::Deny(args) => agents::run_deny(args, data_dir, json_output, id).await,
        CliCommand::Answer(args) => agents::run_answer(args, data_dir, json_output, id).await,
        CliCommand::Ls(args) => reads::run_ls(args, data_dir, json_output).await,
        CliCommand::Top(args) => reads::run_top(args, data_dir, json_output).await,
        CliCommand::Show(args) => reads::run_show(args, data_dir, json_output).await,
        CliCommand::Processes(args) => reads::run_processes(args, data_dir, json_output).await,
        CliCommand::Stats(args) => reads::run_stats(args, data_dir, json_output).await,
        CliCommand::Usage(args) => reads::run_usage(args, data_dir, json_output).await,
        CliCommand::Doctor => reads::run_doctor(data_dir, json_output, doctor_acl_report).await,
        CliCommand::Heavy(args) => reads::run_heavy(args, data_dir, json_output).await,
        CliCommand::Git(args) => git::run(args.command, &data_dir, json_output, id).await,
        CliCommand::GitStatus(args) => reads::run_git_status(args, data_dir, json_output).await,
        CliCommand::PatchDigest(args) => reads::run_patch_digest(args, data_dir, json_output).await,
        CliCommand::Edits(args) => reads::run_edits(args, data_dir, json_output).await,
        CliCommand::Watch(args) => reads::run_watch(args, data_dir, json_output).await,
        CliCommand::Result(args) => reads::run_result(args, data_dir, json_output).await,
        CliCommand::History(args) => reads::run_history(args, data_dir, json_output).await,
        CliCommand::Detach(args) => agents::run_detach(args, data_dir, json_output, id).await,
        CliCommand::Stop(args) => agents::run_stop(args, data_dir, json_output, id).await,
        CliCommand::Configure(args) => agents::run_configure(args, data_dir, json_output, id).await,
        CliCommand::Worktree(args) => agents::run_worktree(args, data_dir, json_output, id).await,
        CliCommand::Cmd(args) => coord::run_cmd(args, data_dir, json_output, id).await,
        CliCommand::Turn(args) => coord::run_turn(args, data_dir, json_output, id).await,
        CliCommand::Why(args) => reads::run_why(args, data_dir, json_output).await,
        CliCommand::Events(args) => reads::run_events(args, data_dir, json_output).await,
        CliCommand::Wait(args) => agents::run_wait(args, data_dir, json_output).await,
        CliCommand::Digest(args) => agents::run_digest(args, data_dir, json_output, id).await,
        CliCommand::Reparent(args) => agents::run_reparent(args, data_dir, json_output, id).await,
        CliCommand::Handoff(args) => agents::run_handoff(args, data_dir, json_output, id).await,
        CliCommand::Switch(args) => agents::run_switch(args, data_dir, json_output, id).await,
        CliCommand::Chat(args) => {
            organization::run_chat(args.command, &data_dir, json_output, id).await
        }
        CliCommand::Msg(args) => coord::run_msg(args, data_dir, json_output, id).await,
        CliCommand::Inbox(args) => coord::run_inbox(args, data_dir, json_output, id).await,
        CliCommand::Var(args) => coord::run_var(args, data_dir, json_output, id).await,
        CliCommand::Who(args) => coord::run_who(args, data_dir, json_output).await,
        CliCommand::Identity => coord::run_identity(data_dir, json_output).await,
        CliCommand::Claim(args) => coord::run_claim(args, data_dir, json_output, id).await,
        CliCommand::Plugin(_) => Err(Failure::new(
            1,
            "plugin commands must be dispatched before CLI dispatch",
        )),
    }
}
