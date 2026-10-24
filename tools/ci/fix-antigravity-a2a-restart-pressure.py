from pathlib import Path

path = Path("modules/antigravity-rust/src/controller.rs")
text = path.read_text(encoding="utf-8")

old = '''        if outcomes.len() > MAX_PENDING_RECEIPTS {
            return Err(swarm_contracts::error::Error::new(
                "MODULE_OUTCOME_OUTBOX_FULL",
                "retained module outcome count exceeds its active bound",
            ));
        }
'''
new = '''        // A prior process may have retained more outcomes than the live
        // admission window before it failed. Startup must drain that bounded
        // durable set rather than fail forever. `may_request_next` keeps new
        // commands closed until the active queue falls below its normal bound.
'''
if text.count(old) != 1:
    raise RuntimeError("retained-outcome startup rejection not found exactly once")
text = text.replace(old, new, 1)

old = '''        if !replacing && self.pending_receipts.len() == MAX_PENDING_RECEIPTS {
'''
new = '''        if !replacing && self.pending_receipts.len() >= MAX_PENDING_RECEIPTS {
'''
if text.count(old) != 1:
    raise RuntimeError("live outcome bound check not found exactly once")
text = text.replace(old, new, 1)

path.write_text(text, encoding="utf-8", newline="\n")
print("A2a restart pressure handling repaired")
