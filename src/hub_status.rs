//! `wm-busbridge hub-status [--json]` — print hub reachability state.
//!
//! Exit 0 if UP, exit 1 if DOWN.

// hub-status uses stdout intentionally.
#![allow(clippy::print_stdout)]

use crate::hub_watcher::{HubState, SharedLiveness};
use anyhow::Result;
use tracing::info;

/// Run the `hub-status` subcommand.
///
/// Reads the shared liveness state and prints a human-readable or JSON
/// summary. Exits with code 0 if UP, 1 if DOWN.
///
/// # Errors
///
/// Returns `Err` if JSON serialization fails (should never happen in practice).
pub async fn run(liveness: SharedLiveness, json: bool) -> Result<()> {
    let locked = liveness.lock().await;
    let state = locked.state;
    let seconds_in_state = locked.seconds_in_state();
    let buffered = locked.buffered_count();
    let dropped = locked.dropped;
    drop(locked);

    if json {
        let out = serde_json::json!({
            "state": state,
            "seconds_in_state": seconds_in_state,
            "buffered": buffered,
            "dropped": dropped
        });
        println!("{}", serde_json::to_string_pretty(&out)?);
    } else {
        println!(
            "hub: {} ({}s in state, {} events buffered, {} dropped)",
            state, seconds_in_state, buffered, dropped
        );
    }

    info!(state = %state, seconds_in_state, buffered, "hub-status");

    match state {
        HubState::Up => std::process::exit(0),
        HubState::Down => std::process::exit(1),
    }
}
