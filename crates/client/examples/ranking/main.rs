//! The reference app of the native SDK: the ranking by votes, through the facade, against a ZemDB
//! server in process on a local socket, with its scenarios. Each scenario runs against a server
//! of its own, on a runtime of its own, and stops at the first check that fails.
//!
//! ```text
//! cargo run -p zemdb-client --example ranking
//! ```
#![allow(clippy::disallowed_methods)]

mod app;
mod scenarios;

fn main() -> app::Result<()> {
    for (what, scenario) in scenarios::ALL {
        println!("{what}");
        tokio::runtime::Runtime::new()?.block_on(scenario())?;
    }
    println!("Every scenario passed");
    Ok(())
}
