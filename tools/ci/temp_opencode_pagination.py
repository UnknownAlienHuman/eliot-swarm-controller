from pathlib import Path

path = Path("crates/swarm-adapter-opencode/src/native.rs")
text = path.read_text(encoding="utf-8")
old = '''            match page.cursor.next {
                None => break,
                Some(next)
                    if !next.is_empty()
                        && next.len() <= 4096
                        && cursors.insert(next.clone()) =>
                {
                    cursor = Some(next);
                }
'''
new = '''            match page.cursor.next {
                None => {
                    // `cursor` names the page request, not unfinished work.
                    // Clear a prior-page cursor when this page proves EOF so a
                    // completed multi-page scan is not reported as limit exhaustion.
                    cursor = None;
                    break;
                }
                Some(next)
                    if !next.is_empty()
                        && next.len() <= 4096
                        && cursors.insert(next.clone()) =>
                {
                    cursor = Some(next);
                }
'''
count = text.count(old)
if count != 1:
    raise SystemExit(f"pagination anchor count={count}")
path.write_text(text.replace(old, new), encoding="utf-8")
