from pathlib import Path

path = Path("crates/swarm-kernel-host/src/store/runtime.rs")
text = path.read_text(encoding="utf-8")
marker = '''    // The normalized dispatch already retained one producer for this exact
    // Operation. Its warm receipt is validated above and retained on the
    // Operation; appending another producer would duplicate its identity.
'''
if text.count(marker) != 1:
    raise SystemExit("normalized warm producer fallback changed")
insert = '''    // A rejected warm terminal is still a closed execution disposition for
    // the normalized dispatch. It must update the same producer before the
    // generic fallback skips normalized admissions.
    if matches!(r.outcome, EffectOutcome::Rejected)
        && o["method"] == "task.dispatch"
        && let Some(admission) = normalized_dispatch_admission.as_ref()
        && warm_producer.is_some()
    {
        producers::record_task_dispatch(
            &tx,
            &o,
            &r,
            admission,
            warm_producer.as_ref(),
            now,
        )?;
    }
'''
text = text.replace(marker, insert + marker, 1)
path.write_text(text, encoding="utf-8")
