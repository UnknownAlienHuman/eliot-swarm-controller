from pathlib import Path

path = Path("crates/swarm-kernel-host/src/store/code_scopes.rs")
text = path.read_text(encoding="utf-8")
old = '''        if record["expires_at_ms"]
            .as_i64()
            .is_some_and(|expires| expires <= now)
        {
            continue;
        }
'''
new = '''        if record["expires_at_ms"]
            .as_i64()
            .is_some_and(|expires| expires <= now)
        {
            // Expiry makes an advisory scope unusable, but it does not prove
            // the retained active writer was explicitly released or otherwise
            // disposed. Omit the stale scope revision and fail coverage closed.
            gaps.push("active_scope_expired_without_release".to_owned());
            continue;
        }
'''
count = text.count(old)
if count != 1:
    raise SystemExit(f"expired active scope anchor count={count}")
path.write_text(text.replace(old, new), encoding="utf-8")
