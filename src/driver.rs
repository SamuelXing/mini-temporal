//! # The driver: replay, match, advance
//!
//! In Phase 0 one struct plays three roles that Temporal splits across a
//! cluster: the **history service** (append events), the **matching service**
//! (dispatch tasks), and the **worker** (replay workflow code, run activities).
//! Collapsing them is the point -- the loop below is short enough to hold in
//! your head, and every later phase is this loop with a network in the middle.
//!
//! One iteration is one *workflow task*:
//!
//! ```text
//!   1. replay   workflow_fn + history  ->  Poll + Vec<Command>
//!   2. verify   new commands == recorded commands, position by position
//!   3. persist  append events for commands history has never seen
//!   4. advance  run the outstanding activities / fire the earliest timer,
//!               append their results
//!   5. discard  throw the future away entirely, go to 1
//! ```
//!
//! ## Why step 1 feeds events one at a time
//!
//! The obvious implementation of replay is a lookup table: build
//! `HashMap<Seq, Outcome>` from history, poll once, let every resolved await
//! return. That is wrong, and wrong in a way that only shows up for workflows
//! that use concurrency.
//!
//! Consider `select2(a, b)` where `b` finished first. Live, the workflow saw
//! only `b` resolved and took `b`'s branch. Later `a` finished too. Now replay
//! with a lookup table: the select polls `a` first, finds it resolved, and
//! takes `a`'s branch. Same code, same history, different answer.
//!
//! The information that was thrown away is *the order the log was written in*.
//! So [`replay_once`] re-delivers recorded results one at a time, in log order,
//! running the workflow to quiescence between each. Replay is not a lookup;
//! it is a re-broadcast.
//!
//! REAL TEMPORAL ~ `sdk-core`'s `WorkflowMachines::apply_next_event`, and the
//! `WorkflowActivation` / job protocol that hands the language side one batch
//! of resolutions at a time rather than the whole history.

use crate::activity::ActivityRegistry;
use crate::command::Command;
use crate::history::{Event, History, Payload};
use crate::workflow::{WfContext, WfState};
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

/// Guard against a workflow that spawns tasks forever without blocking.
/// A workflow that loops forever *inside* one poll cannot be caught here at
/// all; real SDKs use a watchdog thread (Go's `deadlockDetectionTimeout`).
const MAX_ROUNDS: usize = 10_000;

/// Simulated activity latency, so history timestamps advance.
const ACTIVITY_LATENCY_MS: u64 = 10;

/// Where to kill the worker, to prove what survives.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CrashPolicy {
    #[default]
    Never,
    /// Die immediately after the Nth activity's result is durably recorded.
    /// On recovery that activity does NOT run again.
    AfterRecording(usize),
    /// Die after the Nth activity's side effect but *before* its result reaches
    /// history. On recovery that activity DOES run again -- at-least-once.
    BeforeRecording(usize),
}

/// The order in which outstanding activities report back.
///
/// Real workers finish in whatever order the network and the work allow. The
/// engine must not care -- but it must *record* the order it got, because
/// replay has to reproduce it. Flipping this is how the tests prove that.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompletionOrder {
    #[default]
    InOrder,
    Reversed,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RunOutcome {
    Completed(Payload),
    Failed(String),
    /// Simulated process death. History is intact and consistent; call `run`
    /// again with a fresh worker to recover.
    Crashed { after_activity: usize, result_recorded: bool },
    /// Replayed commands diverged from history. This is the error you get in
    /// production when you edit a workflow that has executions in flight.
    NonDeterminism { index: usize, expected: String, actual: String },
    /// The workflow is blocked but nothing is outstanding, so nothing can ever
    /// resolve it. Real Temporal surfaces this as a workflow task timeout.
    Deadlocked,
}

pub struct Worker {
    activities: ActivityRegistry,
    crash: CrashPolicy,
    completion_order: CompletionOrder,
    trace: bool,
    activities_run: Cell<usize>,
    /// Virtual clock. Timers do not sleep; the driver jumps the clock forward,
    /// the same trick Temporal's time-skipping test server uses.
    clock_ms: Cell<u64>,
}

impl Worker {
    pub fn new(activities: ActivityRegistry) -> Self {
        Worker {
            activities,
            crash: CrashPolicy::Never,
            completion_order: CompletionOrder::InOrder,
            trace: false,
            activities_run: Cell::new(0),
            clock_ms: Cell::new(0),
        }
    }

    pub fn with_crash(mut self, crash: CrashPolicy) -> Self {
        self.crash = crash;
        self
    }

    pub fn with_completion_order(mut self, order: CompletionOrder) -> Self {
        self.completion_order = order;
        self
    }

    pub fn with_trace(mut self, trace: bool) -> Self {
        self.trace = trace;
        self
    }

    pub fn activities(&self) -> &ActivityRegistry {
        &self.activities
    }

    /// Drive `wf` against `history` until it completes, crashes, or blocks.
    /// `history` is the durable state; everything else here is disposable.
    pub fn run<F, Fut>(&self, wf: F, history: &mut History) -> RunOutcome
    where
        F: Fn(WfContext) -> Fut,
        Fut: Future<Output = Result<Payload, String>>,
    {
        if let Some(done) = history.result() {
            return match done {
                Ok(r) => RunOutcome::Completed(r),
                Err(e) => RunOutcome::Failed(e),
            };
        }
        // A recovering worker inherits the clock from the log, so time never
        // goes backwards across a restart.
        self.clock_ms.set(self.clock_ms.get().max(history.last_time_ms()));

        let mut attempt = 1u32;
        loop {
            let t = self.tick(1);
            history.append(t, Event::WorkflowTaskStarted { attempt });
            let recorded = history.recorded_commands();

            // ---- 1. replay -------------------------------------------------
            let pass = replay_once(&wf, history);
            self.trace_task(attempt, recorded.len(), &pass);
            if pass.deadlocked {
                return RunOutcome::Deadlocked;
            }

            // ---- 2. verify + 3. persist ------------------------------------
            for (i, cmd) in pass.commands.iter().enumerate() {
                match recorded.get(i) {
                    Some(rec) => {
                        if !cmd.matches(rec) {
                            return RunOutcome::NonDeterminism {
                                index: i,
                                expected: rec.describe(),
                                actual: cmd.describe(),
                            };
                        }
                        // Already durable. Nothing to write.
                    }
                    None => {
                        let t = self.tick(0);
                        history.append(t, cmd.to_event());
                        self.say(format!("      + {}", cmd.describe()));
                    }
                }
            }

            match pass.poll {
                Poll::Ready(Ok(result)) => {
                    let t = self.tick(0);
                    history.append(t, Event::WorkflowExecutionCompleted { result: result.clone() });
                    return RunOutcome::Completed(result);
                }
                Poll::Ready(Err(failure)) => {
                    let t = self.tick(0);
                    history.append(t, Event::WorkflowExecutionFailed { failure: failure.clone() });
                    return RunOutcome::Failed(failure);
                }
                Poll::Pending => {
                    if pass.commands.len() < recorded.len() {
                        return RunOutcome::NonDeterminism {
                            index: pass.commands.len(),
                            expected: recorded[pass.commands.len()].describe(),
                            actual: "<workflow blocked without issuing it>".to_string(),
                        };
                    }
                }
            }

            // ---- 4. advance the world --------------------------------------
            match self.advance(history) {
                Progress::Made => attempt += 1,
                Progress::Stuck => return RunOutcome::Deadlocked,
                Progress::Crashed { after_activity, result_recorded } => {
                    return RunOutcome::Crashed { after_activity, result_recorded }
                }
            }
        }
    }

    /// The non-deterministic half of the system. Everything that touches the
    /// real world happens here, and every outcome is written to history before
    /// the workflow is allowed to observe it.
    fn advance(&self, history: &mut History) -> Progress {
        let resolved = history.resolved_seqs();
        let mut pending: Vec<Command> = history
            .recorded_commands()
            .into_iter()
            .filter(|c| c.is_blocking() && !resolved.contains(&c.seq()))
            .collect();

        if self.completion_order == CompletionOrder::Reversed {
            pending.reverse();
        }

        let mut made = false;

        for cmd in &pending {
            if let Command::ScheduleActivity { seq, activity_type, input } = cmd {
                let n = self.activities_run.get() + 1;
                self.activities_run.set(n);

                self.say(format!("   >> RUN {activity_type}({input})   *** REAL SIDE EFFECT ***"));
                let outcome = self.activities.invoke(activity_type, input);

                if self.crash == CrashPolicy::BeforeRecording(n) {
                    self.say(format!(
                        "   !! WORKER DIED after side effect #{n}, before recording its result"
                    ));
                    return Progress::Crashed { after_activity: n, result_recorded: false };
                }

                let t = self.tick(ACTIVITY_LATENCY_MS);
                match outcome {
                    Ok(result) => {
                        self.say(format!("   << ActivityTaskCompleted [{seq}] -> {result}"));
                        history.append(t, Event::ActivityTaskCompleted { seq: *seq, result });
                    }
                    Err(failure) => {
                        self.say(format!("   << ActivityTaskFailed [{seq}] !! {failure}"));
                        history.append(t, Event::ActivityTaskFailed { seq: *seq, failure });
                    }
                }
                made = true;

                if self.crash == CrashPolicy::AfterRecording(n) {
                    self.say(format!(
                        "   !! WORKER DIED after side effect #{n}, result already durable"
                    ));
                    return Progress::Crashed { after_activity: n, result_recorded: true };
                }
            }
        }

        // Timers only fire when nothing else could move, so a timer used as a
        // timeout loses the race against an activity that is ready. Phase 0
        // policy, not a law: a real server fires whichever deadline comes first.
        if !made {
            let earliest = pending
                .iter()
                .filter_map(|c| match c {
                    Command::StartTimer { seq, fire_after_ms } => Some((*fire_after_ms, *seq)),
                    _ => None,
                })
                .min();
            if let Some((fire_after_ms, seq)) = earliest {
                let t = self.tick(fire_after_ms);
                self.say(format!("   ~~ clock -> {t}ms, TimerFired [{seq}]"));
                history.append(t, Event::TimerFired { seq });
                made = true;
            }
        }

        if made {
            Progress::Made
        } else {
            Progress::Stuck
        }
    }

    fn tick(&self, ms: u64) -> u64 {
        self.clock_ms.set(self.clock_ms.get() + ms);
        self.clock_ms.get()
    }

    fn trace_task(&self, attempt: u32, recorded_len: usize, pass: &Pass) {
        if !self.trace {
            return;
        }
        println!("\n-- workflow task (attempt {attempt}) --");
        let replayed = pass.commands.len().min(recorded_len);
        if replayed > 0 {
            println!("   replayed {replayed} command(s) from history (no side effects):");
            for cmd in pass.commands.iter().take(replayed) {
                println!("      = {}", cmd.describe());
            }
        }
        for line in &pass.logs {
            println!("   [wf log] {line}");
        }
    }

    fn say(&self, line: impl AsRef<str>) {
        if self.trace {
            println!("{}", line.as_ref());
        }
    }
}

enum Progress {
    Made,
    Stuck,
    Crashed { after_activity: usize, result_recorded: bool },
}

struct Pass {
    poll: Poll<Result<Payload, String>>,
    commands: Vec<Command>,
    logs: Vec<String>,
    deadlocked: bool,
}

/// One replay pass: build a brand-new workflow future, feed it the recorded
/// results **in log order**, harvest the commands, throw it away.
fn replay_once<F, Fut>(wf: &F, history: &History) -> Pass
where
    F: Fn(WfContext) -> Fut,
    Fut: Future<Output = Result<Payload, String>>,
{
    let st = Rc::new(RefCell::new(WfState {
        input: history.input(),
        resolved: HashMap::new(),
        markers: history.markers(),
        next_seq: 0,
        commands: Vec::new(),
        recorded_command_count: history.recorded_commands().len(),
        logs: Vec::new(),
        now_ms: 0,
        rng: history.randomness_seed(),
        tasks: Vec::new(),
        progress: 0,
    }));

    let mut root = Box::pin(wf(WfContext::new(st.clone())));
    let mut cx = Context::from_waker(Waker::noop());
    let mut poll = Poll::Pending;

    // Run to the first block, before any result is visible.
    let mut ok = run_until_all_blocked(&st, &mut root, &mut poll, &mut cx);

    // Then re-broadcast every recorded result, one at a time, in log order.
    if ok {
        for r in history.results_in_order() {
            if poll.is_ready() {
                break;
            }
            st.borrow_mut().resolved.insert(r.seq, (r.outcome, r.time_ms));
            ok = run_until_all_blocked(&st, &mut root, &mut poll, &mut cx);
            if !ok {
                break;
            }
        }
    }

    // Everything the workflow "remembered" -- locals, the stack frame parked at
    // `.await`, the spawned tasks -- dies right here. History is the survivor.
    drop(root);

    let snapshot = st.borrow();
    Pass {
        poll,
        commands: snapshot.commands.clone(),
        logs: snapshot.logs.clone(),
        deadlocked: !ok,
    }
}

/// Poll the root future and every detached task, in creation order, until
/// nothing can make progress. Temporal's `ExecuteUntilAllBlocked`.
///
/// For structured concurrency (`join2`, `select2`) this converges in one round,
/// because a composed Rust future is a tree and one `poll` of the root already
/// drives the whole tree. The loop exists for `ctx.spawn`, whose tasks are
/// detached and can unblock each other.
fn run_until_all_blocked<Fut>(
    st: &Rc<RefCell<WfState>>,
    root: &mut Pin<Box<Fut>>,
    outcome: &mut Poll<Result<Payload, String>>,
    cx: &mut Context<'_>,
) -> bool
where
    Fut: Future<Output = Result<Payload, String>>,
{
    for _ in 0..MAX_ROUNDS {
        let before = st.borrow().progress;

        if outcome.is_pending() {
            *outcome = root.as_mut().poll(cx);
        }

        // Detached tasks, polled in creation order. Deterministic by
        // construction: no randomised ready-queue, no work stealing.
        let taken = std::mem::take(&mut st.borrow_mut().tasks);
        let mut still = Vec::with_capacity(taken.len());
        for mut t in taken {
            if t.as_mut().poll(cx).is_pending() {
                still.push(t);
            } else {
                st.borrow_mut().progress += 1;
            }
        }
        {
            let mut m = st.borrow_mut();
            still.append(&mut m.tasks); // tasks spawned during this round go last
            m.tasks = still;
        }

        if outcome.is_ready() {
            return true;
        }
        if st.borrow().progress == before {
            return true; // quiescent: every coroutine is blocked
        }
    }
    false
}
