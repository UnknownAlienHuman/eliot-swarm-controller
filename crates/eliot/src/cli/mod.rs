//! Thin dispatch for the `swarm` CLI.
//! Each command family owns its `CliCommand` arms outright; this
//! module only routes a parsed command to the owning family runner.
mod agents;
pub(crate) mod args;
mod coord;
mod daemon;
mod reads;
mod rules;
pub(crate) mod support;

use self::args::CliCommand;
use self::support::Failure;
use std::path::PathBuf;

pub(crate) async fn dispatch(
    command: CliCommand,
    data_dir: PathBuf,
    json_output: bool,
    id: Option<String>,
) -> Result<u8, Failure> {
    match command {
        CliCommand::Daemon(args) => daemon::run_daemon(args, data_dir, json_output, id).await,
        CliCommand::Mcp(args) => daemon::run_mcp(args, data_dir, json_output).await,
        CliCommand::Status => daemon::run_status(data_dir, json_output).await,
        CliCommand::Provider(args) => rules::run_provider(args, data_dir, json_output, id).await,
        CliCommand::Project(args) => rules::run_project(args, data_dir, json_output).await,
        CliCommand::Rule(args) => rules::run_rule(args, data_dir, json_output, id).await,
        CliCommand::Timer(args) => rules::run_timer(args, data_dir, json_output, id).await,
        CliCommand::Goal(args) => rules::run_goal(args, data_dir, json_output, id).await,
        CliCommand::Discover(args) => agents::run_discover(args, data_dir, json_output).await,
        CliCommand::Harnesses => agents::run_harnesses(data_dir, json_output).await,
        CliCommand::Models(args) => agents::run_models(args, data_dir, json_output).await,
        CliCommand::Attach(args) => agents::run_attach(args, data_dir, json_output, id).await,
        CliCommand::Launch(args) => agents::run_launch(args, data_dir, json_output, id).await,
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
        CliCommand::GitStatus(args) => reads::run_git_status(args, data_dir, json_output).await,
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
        CliCommand::Events(args) => reads::run_events(args, data_dir, json_output).await,
        CliCommand::Wait(args) => agents::run_wait(args, data_dir, json_output).await,
        CliCommand::Digest(args) => agents::run_digest(args, data_dir, json_output, id).await,
        CliCommand::Msg(args) => coord::run_msg(args, data_dir, json_output, id).await,
        CliCommand::Inbox(args) => coord::run_inbox(args, data_dir, json_output).await,
        CliCommand::Var(args) => coord::run_var(args, data_dir, json_output, id).await,
        CliCommand::Who(args) => coord::run_who(args, data_dir, json_output).await,
        CliCommand::Claim(args) => coord::run_claim(args, data_dir, json_output, id).await,
        CliCommand::Plugin(_) => Err(Failure::new(
            1,
            "plugin commands must be dispatched before CLI dispatch",
        )),
    }
}
