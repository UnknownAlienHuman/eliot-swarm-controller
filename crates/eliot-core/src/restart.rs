//! Restart continuation note: the framed Swarm note that opens the resumed
//! session of an Agent whose turn a daemon restart interrupted. The
//! interrupted turn is named, never replayed.

/// Principal carried by the restart continuation note. It renders as the
/// `Swarm` sender in [`crate::swarm_note`], beside the background-wake
/// principal.
pub const RESTART_PRINCIPAL: &str = "swarm_note:daemon_restart";

/// Format the body of the Swarm note delivered as the first input of a
/// resume admitted for a restart-interrupted generation. `turn_id` names the
/// interrupted turn when the `agent.lost` event retained one; without it the
/// note stays generic. The body carries only Store facts (Agent, generation,
/// turn), never harness transcript text.
pub fn format_restart_interrupted_turn_note_body(
    agent: &str,
    generation: i64,
    turn_id: Option<&str>,
) -> String {
    let interruption = match turn_id {
        Some(turn) => format!("turn {turn} of generation {generation}"),
        None => format!("generation {generation} before its turn was tracked"),
    };
    format!(
        "A daemon restart cut off {agent} while {interruption} was running. \
         That turn's outcome is unknown and it will not be replayed: do not assume \
         any of its tool calls, edits, commits or pushes happened. \
         Reconcile the worktree first (git status and git log on the task branch, \
         plus the task card), then continue the task from the files as they are."
    )
}
