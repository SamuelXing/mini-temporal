//! # mini-temporal
//!
//! A durable execution engine, written from scratch to understand how Temporal,
//! Cadence and Azure DurableTask actually work. They share one core idea:
//!
//! > **A workflow is a pure function of its event history.**
//! >
//! > `commands = replay(workflow_fn, history)`
//!
//! Recovery is not a feature bolted on top. Recovery *is* the execution model:
//! the engine re-runs the workflow function from the top and feeds it the
//! recorded history, so every step that already has a result returns that
//! result instead of doing the work again. When the replay catches up with the
//! end of the history, execution goes live and new commands come out.
//!
//! ## Where to read
//!
//! 1. [`workflow`] -- how `.await` suspends without ever being woken.
//! 2. [`driver`]   -- the replay/verify/persist/advance loop. This is the engine.
//! 3. [`history`]  -- the only durable state in the system.
//! 4. [`command`]  -- why intents and facts are different types.
//!
//! ## What Phase 0 deliberately leaves out
//!
//! Persistence, a network, retries, signals, queries, child workflows,
//! cancellation, `continue_as_new`, versioning, the sticky cache. See README.md
//! for the phase plan. None of them change the core loop; all of them are
//! layered on it.

pub mod activity;
pub mod combinators;
pub mod command;
pub mod driver;
pub mod history;
pub mod workflow;

pub mod prelude {
    pub use crate::activity::{ActivityRegistry, Execution};
    pub use crate::combinators::{join2, join_all, select2, Either};
    pub use crate::command::Command;
    pub use crate::driver::{CompletionOrder, CrashPolicy, RunOutcome, Worker};
    pub use crate::history::{Event, History, HistoryEvent, Outcome, Payload, Seq};
    pub use crate::workflow::{ActivityFailure, WfContext};
}
