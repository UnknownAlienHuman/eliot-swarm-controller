#!/usr/bin/env python3
from pathlib import Path


def replace_exact(path: str, old: str, new: str) -> None:
    file = Path(path)
    text = file.read_text(encoding="utf-8")
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected one replacement, found {count}")
    file.write_text(text.replace(old, new), encoding="utf-8")


def main() -> None:
    automation = Path("crates/swarm-automation/src/lib.rs")
    code_scopes = Path("crates/swarm-kernel-host/src/store/code_scopes.rs")
    if (
        "return swarm_process::departed_empty(&identity, service_owner_token);"
        in automation.read_text(encoding="utf-8")
        and "tasks::get_attempt(tx, attempt_id)?;"
        in code_scopes.read_text(encoding="utf-8")
    ):
        print("patch already applied")
        return

    replace_exact(
        str(automation),
        '''    let identity = identity.clone();
    #[cfg(windows)]
    {
        identity["job_name"] = serde_json::json!(format!(
            "Global\\\\EliotSwarmService-AutomationScheduler-{service_owner_token}"
        ));
    }
    swarm_process::departed_empty(&identity, service_owner_token)
''',
        '''    #[cfg(windows)]
    {
        let mut identity = identity.clone();
        identity["job_name"] = serde_json::json!(format!(
            "Global\\\\EliotSwarmService-AutomationScheduler-{service_owner_token}"
        ));
        return swarm_process::departed_empty(&identity, service_owner_token);
    }
    #[cfg(not(windows))]
    {
        swarm_process::departed_empty(identity, service_owner_token)
    }
''',
    )
    replace_exact(
        str(code_scopes),
        '''    let attempt = tasks::get_attempt(tx, &attempt_id)?;
    let scope_intent_id = format!("cscope-{}", model::new_id());
    let sequence = next_task_sequence(tx, &task_id)?;
''',
        '''    tasks::get_attempt(tx, attempt_id)?;
    let scope_intent_id = format!("cscope-{}", model::new_id());
    let sequence = next_task_sequence(tx, task_id)?;
''',
    )
    replace_exact(
        str(code_scopes),
        "        &task_page_key(&task_id, sequence)?,\n",
        "        &task_page_key(task_id, sequence)?,\n",
    )


if __name__ == "__main__":
    main()
