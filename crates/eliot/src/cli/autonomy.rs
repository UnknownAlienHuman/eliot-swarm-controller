use super::{args::AutonomyArgs, support::Failure};
use eliot_core::api::{AutonomyRequest, AutonomyResponse, AutonomyRow};
use std::{
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

pub(super) async fn run(args: AutonomyArgs, data_dir: &Path, json: bool) -> Result<u8, Failure> {
    let request = AutonomyRequest {
        since_ms: args.since.as_deref().map(parse_since).transpose()?,
    };
    let report: AutonomyResponse =
        eliot_daemon::client_call_typed(data_dir, "autonomy.get", &request)
            .await
            .map_err(Failure::for_read)?;
    if json {
        println!(
            "{}",
            serde_json::to_string(&report)
                .map_err(|_| Failure::new(4, "autonomy report could not be encoded"))?
        );
    } else {
        print_report(&report);
    }
    Ok(0)
}

fn parse_since(value: &str) -> Result<i64, Failure> {
    let time = if let Ok(millis) = value.parse::<i64>() {
        return Ok(millis);
    } else if let Ok(time) = humantime::parse_rfc3339(value) {
        Some(time)
    } else if let Ok(duration) = humantime::parse_duration(value) {
        SystemTime::now().checked_sub(duration)
    } else {
        None
    };
    time.and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .and_then(|duration| i64::try_from(duration.as_millis()).ok())
        .ok_or_else(|| {
            Failure::new(
                2,
                "--since expects RFC 3339, Unix milliseconds, or a duration",
            )
        })
}

fn timestamp(millis: i64) -> String {
    u64::try_from(millis)
        .ok()
        .and_then(|millis| UNIX_EPOCH.checked_add(Duration::from_millis(millis)))
        .map_or_else(
            || millis.to_string(),
            |time| humantime::format_rfc3339_millis(time).to_string(),
        )
}

fn row<T>(title: &str, row: &AutonomyRow<T>, print_counts: impl FnOnce(&T)) {
    println!("{title} [{} | {}]", row.coverage.as_str(), row.source);
    if let Some(counts) = &row.counts {
        print_counts(counts);
    }
    if let Some(reason) = &row.reason {
        println!("  {reason}");
    }
}

fn print_report(report: &AutonomyResponse) {
    println!(
        "Swarm autonomy from {} to {}",
        timestamp(report.since_ms),
        timestamp(report.until_ms)
    );
    row("Project policy", &report.project_policy, |counts| {
        println!(
            "  Policy applied to {} of {} created Agents, {} opted out",
            counts.applied, counts.created, counts.opted_out
        );
        for project in &counts.projects {
            println!(
                "  {}  {} of {} applied, {} opted out",
                project.project, project.applied, project.created, project.opted_out
            );
        }
        if counts.projects_truncated {
            println!("  More projects omitted");
        }
        for opt_out in &counts.opt_outs {
            println!(
                "  {} ({}) opted out — {}",
                opt_out.agent, opt_out.project, opt_out.reason
            );
        }
        if counts.opt_outs_truncated {
            println!("  More opt-outs omitted");
        }
    });
    row("Steers", &report.steer, |counts| {
        println!("  {} manual, {} Swarm", counts.manual, counts.swarm);
    });
    row("Checks", &report.checks, |counts| {
        println!("  {} manual, {} Swarm", counts.manual, counts.swarm);
    });
    row("Shell waits", &report.waits, |counts| {
        println!("  {} calls", counts.calls)
    });
    row("Swarm fact notes", &report.notes, |counts| {
        println!(
            "  {} delivered, {} moved, {} cancelled, {} expired",
            counts.delivered, counts.moved, counts.cancelled, counts.expired
        );
        for kind in &counts.by_kind {
            println!(
                "  {}  {} delivered, {} moved, {} cancelled, {} expired, {} cleared",
                kind.kind, kind.delivered, kind.moved, kind.cancelled, kind.expired, kind.resolved
            );
        }
    });
    row(
        "Waiting for the owner over 24h",
        &report.owner_waiting,
        |counts| {
            println!("  {} items", counts.total);
            for (kind, count) in &counts.by_kind {
                if *count > 0 {
                    println!("  {} {}", count, kind.as_str().replace('_', " "));
                }
            }
            for item in &counts.oldest {
                println!(
                    "  {} {} since {}",
                    item.kind,
                    item.id,
                    timestamp(item.since_ms)
                );
            }
            if counts.oldest_truncated {
                println!("  More items omitted");
            }
        },
    );
    row("Normalized usage", &report.usage, |counts| {
        let share = counts.manager_input_share.map_or_else(
            || "unavailable".into(),
            |value| format!("{:.1}%", value * 100.0),
        );
        let context = counts.average_context_tokens.map_or_else(
            || "unavailable".into(),
            |value| format!("{value:.1} tokens"),
        );
        println!(
            "  Managers {share} of input ({} tokens), executors {} input tokens",
            counts.manager_input_tokens, counts.executor_input_tokens
        );
        println!(
            "  Average context per call {context}, {} observed calls",
            counts.observed_calls
        );
    });
    row(
        "Executors calling Swarm",
        &report.executor_swarm,
        |counts| {
            if counts.profiles.is_empty() {
                println!("  No executor activity observed");
            }
            for profile in &counts.profiles {
                println!(
                    "  {}  {} of {} called Swarm",
                    profile.profile, profile.called_swarm, profile.executors
                );
            }
            if counts.profiles_truncated {
                println!("  More profiles omitted");
            }
        },
    );
}
