//! Two separate guarantees, tested separately.
//!
//! **A -- identity is anchored to an order the *language* guarantees.**
//!   `seq` is handed out when `ctx.activity(..)` is called, so it follows Rust's
//!   specified expression evaluation order, not a combinator's poll order.
//!
//! **B -- the runtime order is NOT reproducible, and must not need to be.**
//!   The engine records whatever interleaving the world produced and replays
//!   *that*. `completion_order_*` proves the recorded order is what decides,
//!   and that a different worker replaying the same log reaches the same answer.

use mini_temporal::prelude::*;
use std::cell::{Cell, RefCell};
use std::rc::Rc;

type Log = Rc<RefCell<Vec<Execution>>>;

thread_local! {
    /// Stands in for anything outside history: a real clock, a real RNG,
    /// process-global state.
    static IMPURE: Cell<u32> = const { Cell::new(0) };
}

fn worker(log: Log) -> Worker {
    let mut reg = ActivityRegistry::with_log(log);
    reg.register("a", |i| Ok(format!("a:{i}")))
        .register("b", |i| Ok(format!("b:{i}")))
        .register("bg", |i| Ok(format!("bg:{i}")))
        .register("main", |i| Ok(format!("main:{i}")))
        .register("record", |i| Ok(format!("recorded({i})")));
    Worker::new(reg)
}

fn count(log: &Log, name: &str) -> usize {
    log.borrow().iter().filter(|e| e.activity_type == name).count()
}

fn scheduled(history: &History) -> Vec<String> {
    history
        .events()
        .iter()
        .filter_map(|e| match &e.attrs {
            Event::ActivityTaskScheduled { seq, activity_type, input } => {
                Some(format!("{seq}:{activity_type}({input})"))
            }
            _ => None,
        })
        .collect()
}

fn completed(history: &History) -> Vec<u32> {
    history
        .events()
        .iter()
        .filter_map(|e| match &e.attrs {
            Event::ActivityTaskCompleted { seq, .. } => Some(*seq),
            _ => None,
        })
        .collect()
}

// ===========================================================================
// A. Identity
// ===========================================================================

/// Create `a` then `b`, but await `b` first. Sequence numbers must follow the
/// *creation* order, because that is the order Rust specifies. If they followed
/// poll order, any combinator that reorders polling -- `tokio::select!`
/// randomises by default -- would silently renumber every await point.
#[test]
fn seq_follows_creation_order_not_poll_order() {
    async fn wf(ctx: WfContext) -> Result<Payload, String> {
        let a = ctx.activity("a", "first-created");
        let b = ctx.activity("b", "second-created");
        let rb = b.await.map_err(|e| e.to_string())?; // awaited first
        let ra = a.await.map_err(|e| e.to_string())?;
        Ok(format!("{ra}|{rb}"))
    }

    let log: Log = Rc::default();
    let mut h = History::start("x");
    let outcome = worker(log).run(wf, &mut h);

    assert_eq!(outcome, RunOutcome::Completed("a:first-created|b:second-created".into()));
    assert_eq!(
        scheduled(&h),
        vec!["0:a(first-created)".to_string(), "1:b(second-created)".to_string()],
        "seq must follow creation order"
    );
}

/// `ctx.now_ms()` reads history, so a crash in the middle changes nothing.
#[test]
fn workflow_time_is_replay_stable() {
    async fn wf(ctx: WfContext) -> Result<Payload, String> {
        let t0 = ctx.now_ms();
        ctx.activity("a", "x").await.map_err(|e| e.to_string())?;
        let t1 = ctx.now_ms();
        ctx.timer(500).await;
        Ok(format!("{t0},{t1},{}", ctx.now_ms()))
    }

    let clean: Log = Rc::default();
    let mut h1 = History::start("x");
    let a = worker(clean).run(wf, &mut h1);

    let crashed: Log = Rc::default();
    let mut h2 = History::start("x");
    worker(crashed.clone()).with_crash(CrashPolicy::AfterRecording(1)).run(wf, &mut h2);
    let b = worker(crashed).run(wf, &mut h2);

    assert_eq!(a, b, "workflow-visible time must survive a crash unchanged");
    assert!(matches!(a, RunOutcome::Completed(ref s) if s.starts_with("0,")));
}

/// Same for `ctx.random_u64()`: the seed lives in `WorkflowExecutionStarted`.
#[test]
fn workflow_randomness_is_replay_stable() {
    async fn wf(ctx: WfContext) -> Result<Payload, String> {
        let x = ctx.random_u64();
        ctx.activity("a", "x").await.map_err(|e| e.to_string())?;
        let y = ctx.random_u64();
        Ok(format!("{x},{y}"))
    }

    let l1: Log = Rc::default();
    let mut h1 = History::start("x");
    let clean = worker(l1).run(wf, &mut h1);

    let l2: Log = Rc::default();
    let mut h2 = History::start("x");
    worker(l2.clone()).with_crash(CrashPolicy::AfterRecording(1)).run(wf, &mut h2);
    let recovered = worker(l2).run(wf, &mut h2);

    assert_eq!(clean, recovered);
}

/// `side_effect` freezes an arbitrary local computation into a marker event.
/// The closure runs once, ever -- not once per replay.
#[test]
fn side_effect_runs_once_and_is_frozen_in_history() {
    async fn wf(ctx: WfContext) -> Result<Payload, String> {
        let id = ctx.side_effect("request_id", || {
            IMPURE.with(|c| c.set(c.get() + 1));
            format!("req-{}", IMPURE.with(|c| c.get()))
        });
        let r = ctx.activity("a", id.as_str()).await.map_err(|e| e.to_string())?;
        Ok(format!("{id}|{r}"))
    }

    IMPURE.with(|c| c.set(0));
    let log: Log = Rc::default();
    let mut h = History::start("x");
    worker(log.clone()).with_crash(CrashPolicy::AfterRecording(1)).run(wf, &mut h);
    let outcome = worker(log).run(wf, &mut h);

    assert_eq!(outcome, RunOutcome::Completed("req-1|a:req-1".into()));
    assert_eq!(IMPURE.with(|c| c.get()), 1, "closure must not re-run on replay");
    assert!(h.events().iter().any(|e| matches!(e.attrs, Event::MarkerRecorded { .. })));
}

/// The honest limit of the non-determinism check.
///
/// This workflow reads process-global state but always emits the *same command
/// shape*. Position-by-position command matching cannot see the difference, so
/// the engine reports success while the workflow computes a different answer
/// than it did live. Nothing short of a sandbox catches this class -- which is
/// why the TypeScript and Python SDKs ship one, and why Go and Java tell you
/// not to do it.
#[test]
fn impure_code_with_stable_command_shape_is_not_caught() {
    async fn wf(ctx: WfContext) -> Result<Payload, String> {
        let n = IMPURE.with(|c| {
            c.set(c.get() + 1);
            c.get()
        });
        ctx.activity("a", "x").await.map_err(|e| e.to_string())?;
        Ok(format!("replay-pass={n}"))
    }

    IMPURE.with(|c| c.set(0));
    let l1: Log = Rc::default();
    let mut h1 = History::start("x");
    let clean = worker(l1).run(wf, &mut h1);

    IMPURE.with(|c| c.set(0));
    let l2: Log = Rc::default();
    let mut h2 = History::start("x");
    worker(l2.clone()).with_crash(CrashPolicy::BeforeRecording(1)).run(wf, &mut h2);
    let recovered = worker(l2).run(wf, &mut h2);

    assert!(matches!(clean, RunOutcome::Completed(_)));
    assert!(matches!(recovered, RunOutcome::Completed(_)), "no error is raised");
    assert_ne!(clean, recovered, "and yet the answers differ -- this is the known gap");
}

// ===========================================================================
// B. Runtime order
// ===========================================================================

async fn race(ctx: WfContext) -> Result<Payload, String> {
    let a = ctx.activity("a", "x");
    let b = ctx.activity("b", "x");
    let winner = match select2(a, b).await {
        Either::Left(r) => format!("A:{}", r.map_err(|e| e.to_string())?),
        Either::Right(r) => format!("B:{}", r.map_err(|e| e.to_string())?),
    };
    ctx.activity("record", winner.as_str()).await.map_err(|e| e.to_string())
}

/// Flip which worker reports back first and the recorded interleaving flips
/// with it -- as it should. The engine imposes no order on the world.
#[test]
fn the_recorded_interleaving_decides_the_winner() {
    let l1: Log = Rc::default();
    let mut h1 = History::start("x");
    let in_order = worker(l1).with_completion_order(CompletionOrder::InOrder).run(race, &mut h1);

    let l2: Log = Rc::default();
    let mut h2 = History::start("x");
    let reversed = worker(l2).with_completion_order(CompletionOrder::Reversed).run(race, &mut h2);

    assert_eq!(completed(&h1)[..2], [0, 1], "a reported first");
    assert_eq!(completed(&h2)[..2], [1, 0], "b reported first");
    assert_eq!(in_order, RunOutcome::Completed("recorded(A:a:x)".into()));
    assert_eq!(reversed, RunOutcome::Completed("recorded(B:b:x)".into()));
}

/// The one that makes replay faithful.
///
/// Worker 1 loses the race to `b` and dies. Worker 2 is a different process
/// with a different completion policy, and replays a history in which BOTH
/// racers are already resolved. It must still pick `b`, because that is the
/// order the log records.
///
/// A lookup-table replay (`HashMap<Seq, Outcome>`, poll once) answers `A` here:
/// `select2` polls the left branch first and finds it resolved. Same code, same
/// history, different answer. That bug is invisible until a workflow uses
/// concurrency, which is why it is worth a test of its own.
#[test]
fn replay_reproduces_the_recorded_race_not_the_poll_order() {
    let log: Log = Rc::default();
    let mut h = History::start("x");

    let first = worker(log.clone())
        .with_completion_order(CompletionOrder::Reversed)
        .with_crash(CrashPolicy::AfterRecording(2))
        .run(race, &mut h);
    assert!(matches!(first, RunOutcome::Crashed { .. }));
    assert_eq!(completed(&h), vec![1, 0], "b's result was written first");

    // Fresh worker, default (in-order) policy, both racers already resolved.
    let recovered = worker(log.clone()).run(race, &mut h);

    assert_eq!(recovered, RunOutcome::Completed("recorded(B:b:x)".into()));
    assert_eq!(count(&log, "a"), 1);
    assert_eq!(count(&log, "b"), 1);
}

/// Detached coroutines: the root and every spawned task are polled to
/// quiescence in creation order, so their commands interleave deterministically.
#[test]
fn spawned_tasks_run_until_all_blocked_in_creation_order() {
    async fn wf(ctx: WfContext) -> Result<Payload, String> {
        for i in 0..3u32 {
            let c = ctx.clone();
            ctx.spawn(async move {
                let _ = c.activity("bg", i.to_string()).await;
            });
        }
        ctx.activity("main", "x").await.map_err(|e| e.to_string())
    }

    let log: Log = Rc::default();
    let mut h = History::start("x");
    let outcome = worker(log.clone()).run(wf, &mut h);

    assert_eq!(outcome, RunOutcome::Completed("main:x".into()));
    // Root blocks first (seq 0), then the three detached tasks in spawn order.
    assert_eq!(
        scheduled(&h),
        vec![
            "0:main(x)".to_string(),
            "1:bg(0)".to_string(),
            "2:bg(1)".to_string(),
            "3:bg(2)".to_string(),
        ]
    );
    assert_eq!(count(&log, "bg"), 3);
}
