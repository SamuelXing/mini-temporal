//! # The workflow side of the boundary
//!
//! Two separate guarantees live in this file, and confusing them is how people
//! get durable execution wrong.
//!
//! ## 1. Identity: which await point is this?
//!
//! `ctx.activity(..)` takes the next [`Seq`] at **call** time, so identity is
//! anchored to Rust's specified expression evaluation order -- a *language*
//! guarantee. The tempting alternative, assigning on first `poll`, anchors it
//! to whichever order a combinator happens to poll its branches in: a *library*
//! detail. `tokio::select!` randomises that order by default. Anchoring
//! identity there would be catastrophic.
//!
//! The price is that these futures are not lazy. Creating one issues the
//! command even if you drop it without awaiting. Temporal's real Rust SDK has
//! the same property, for the same reason.
//!
//! ## 2. Values: is everything this code reads a function of history?
//!
//! Identity being stable is not enough. If the workflow calls
//! `SystemTime::now()` or `rand::random()`, the command *shape* can stay
//! identical while the computed values differ -- and no replay engine can
//! detect that. So every source of non-determinism needs a history-backed
//! replacement: [`WfContext::now_ms`], [`WfContext::random_u64`], and the
//! general escape hatch [`WfContext::side_effect`].
//!
//! Nothing here *prevents* a workflow from reaching for the real clock. Real
//! SDKs that can do so (TypeScript, Python) sandbox the workflow and replace
//! the globals; Go and Java rely on convention plus detection, as we do.
//!
//! ## The suspension trick
//!
//! There is no waker. Nothing ever completes an [`ActivityFuture`]; it returns
//! `Pending` forever. The workflow's state *is* the suspended stack frame, and
//! the driver discards it and rebuilds it by replaying. See `driver.rs`.
//!
//! REAL TEMPORAL ~ `sdk-core/sdk/src/workflow_context.rs` (`WfContext`).

use crate::command::Command;
use crate::history::{Outcome, Payload, Seq};
use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

#[derive(Debug, Clone, PartialEq)]
pub struct ActivityFailure(pub String);

impl std::fmt::Display for ActivityFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "activity failed: {}", self.0)
    }
}

/// A detached workflow coroutine, the equivalent of `workflow.Go` /
/// `asyncio.create_task`. Polled by the driver alongside the root future.
pub(crate) type WfTask = Pin<Box<dyn Future<Output = ()>>>;

/// State shared between the context handle and every future it hands out.
/// Lives for exactly one replay pass, then is dropped.
pub(crate) struct WfState {
    pub(crate) input: Payload,
    /// Results the workflow has been allowed to observe *so far this pass*.
    /// The driver fills this in one entry at a time, in history order.
    pub(crate) resolved: HashMap<Seq, (Outcome, u64)>,
    /// Values recorded by `side_effect`, from `MarkerRecorded` events.
    pub(crate) markers: HashMap<Seq, Payload>,
    pub(crate) next_seq: Seq,
    pub(crate) commands: Vec<Command>,
    /// How many commands history already recorded. While we have produced
    /// fewer than this, we are re-deriving the past, i.e. replaying.
    pub(crate) recorded_command_count: usize,
    pub(crate) logs: Vec<String>,
    /// Workflow-visible time. Monotonic, derived only from event timestamps.
    pub(crate) now_ms: u64,
    /// PRNG state, seeded from `WorkflowExecutionStarted.randomness_seed`.
    pub(crate) rng: u64,
    /// Detached coroutines waiting to be polled.
    pub(crate) tasks: Vec<WfTask>,
    /// Bumped on anything that counts as forward progress. The driver uses it
    /// to decide when the workflow has run until *all* coroutines are blocked.
    pub(crate) progress: u64,
}

/// The only thing a workflow function is given. Deliberately tiny: if it isn't
/// on here, the workflow should not be doing it.
#[derive(Clone)]
pub struct WfContext {
    st: Rc<RefCell<WfState>>,
}

impl WfContext {
    pub(crate) fn new(st: Rc<RefCell<WfState>>) -> Self {
        WfContext { st }
    }

    pub fn input(&self) -> Payload {
        self.st.borrow().input.clone()
    }

    /// Ask the server to run an activity. Returns a future that only ever
    /// resolves from history.
    pub fn activity(&self, activity_type: &str, input: impl Into<Payload>) -> ActivityFuture {
        let seq = self.issue(|seq| Command::ScheduleActivity {
            seq,
            activity_type: activity_type.to_string(),
            input: input.into(),
        });
        ActivityFuture { seq, st: self.st.clone() }
    }

    /// Ask the server for a durable timer. Survives process restarts, unlike
    /// `sleep`, because it is a command and its firing is an event.
    pub fn timer(&self, fire_after_ms: u64) -> TimerFuture {
        let seq = self.issue(|seq| Command::StartTimer { seq, fire_after_ms });
        TimerFuture { seq, st: self.st.clone() }
    }

    /// Workflow-visible time.
    ///
    /// Not the wall clock. It is the timestamp of the last history event this
    /// pass has observed, so it is monotonic, identical on every replay, and
    /// frozen while the workflow is not waiting on anything. Calling
    /// `SystemTime::now()` in a workflow instead is the single most common way
    /// to break replay -- and one that command-shape checking cannot catch.
    ///
    /// REAL TEMPORAL ~ `workflow.Now()` (Go), which returns the
    /// `WorkflowTaskStarted` event time of the task being replayed.
    pub fn now_ms(&self) -> u64 {
        self.st.borrow().now_ms
    }

    /// Deterministic randomness, seeded from history.
    ///
    /// Same sequence on every replay, because the seed is in
    /// `WorkflowExecutionStarted` and the call order is deterministic.
    pub fn random_u64(&self) -> u64 {
        let mut st = self.st.borrow_mut();
        st.rng = st.rng.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = st.rng;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Run `f` once, record its result forever. The general escape hatch for
    /// non-determinism that is not worth a full activity round-trip.
    ///
    /// Unlike an activity this does **not** block: the value is computed inline
    /// during the workflow task and shipped out as a `RecordMarker` command,
    /// which the server turns into a `MarkerRecorded` event. On every later
    /// replay the recorded value is returned and `f` is never called.
    ///
    /// Caveat, shared with real Temporal: if the worker dies before the marker
    /// is persisted, `f` runs again. It is at-least-once, like an activity --
    /// just cheaper.
    ///
    /// REAL TEMPORAL ~ `workflow.SideEffect` (Go) / `MarkerRecorded` events.
    /// `patched()` / `GetVersion` is built on the same mechanism.
    pub fn side_effect(&self, marker_name: &str, f: impl FnOnce() -> Payload) -> Payload {
        let mut st = self.st.borrow_mut();
        let seq = st.next_seq;
        st.next_seq += 1;
        let value = match st.markers.get(&seq) {
            Some(recorded) => recorded.clone(),
            None => f(),
        };
        st.commands.push(Command::RecordMarker {
            seq,
            marker_name: marker_name.to_string(),
            value: value.clone(),
        });
        st.progress += 1;
        value
    }

    /// Start a detached workflow coroutine.
    ///
    /// The scheduling equivalent of `workflow.Go` (Go SDK) or
    /// `asyncio.create_task` (Python SDK). The driver polls the root future and
    /// every spawned task in creation order, repeatedly, until nothing can make
    /// progress -- Temporal's `ExecuteUntilAllBlocked`.
    pub fn spawn(&self, fut: impl Future<Output = ()> + 'static) {
        let mut st = self.st.borrow_mut();
        st.tasks.push(Box::pin(fut));
        st.progress += 1;
    }

    /// True while this pass is re-deriving commands that history already has.
    ///
    /// The one legitimate use is suppressing side effects that are not under
    /// the engine's control -- logs, metrics. Branching workflow *logic* on
    /// this introduces non-determinism by construction.
    pub fn is_replaying(&self) -> bool {
        let st = self.st.borrow();
        st.commands.len() < st.recorded_command_count
    }

    /// Logs once per logical execution rather than once per replay.
    pub fn log(&self, msg: impl Into<String>) {
        if self.is_replaying() {
            return;
        }
        self.st.borrow_mut().logs.push(msg.into());
    }

    fn issue(&self, make: impl FnOnce(Seq) -> Command) -> Seq {
        let mut st = self.st.borrow_mut();
        let seq = st.next_seq;
        st.next_seq += 1;
        let cmd = make(seq);
        st.commands.push(cmd);
        st.progress += 1;
        seq
    }
}

/// Advance workflow-visible time to an observed event's timestamp.
fn observe(st: &Rc<RefCell<WfState>>, at_ms: u64) {
    let mut st = st.borrow_mut();
    if at_ms > st.now_ms {
        st.now_ms = at_ms;
    }
}

/// Resolves from `ActivityTaskCompleted` / `ActivityTaskFailed` in history.
pub struct ActivityFuture {
    seq: Seq,
    st: Rc<RefCell<WfState>>,
}

impl ActivityFuture {
    pub fn seq(&self) -> Seq {
        self.seq
    }
}

impl Future for ActivityFuture {
    type Output = Result<Payload, ActivityFailure>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        // No waker registration. Nothing wakes us; the driver re-polls.
        let found = self.st.borrow().resolved.get(&self.seq).cloned();
        match found {
            Some((outcome, at_ms)) => {
                observe(&self.st, at_ms);
                match outcome {
                    Outcome::Completed(p) => Poll::Ready(Ok(p)),
                    Outcome::Failed(f) => Poll::Ready(Err(ActivityFailure(f))),
                }
            }
            None => Poll::Pending,
        }
    }
}

/// Resolves from `TimerFired` in history.
pub struct TimerFuture {
    seq: Seq,
    st: Rc<RefCell<WfState>>,
}

impl Future for TimerFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let found = self.st.borrow().resolved.get(&self.seq).map(|(_, t)| *t);
        match found {
            Some(at_ms) => {
                observe(&self.st, at_ms);
                Poll::Ready(())
            }
            None => Poll::Pending,
        }
    }
}
