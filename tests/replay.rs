//! The properties Phase 0 is supposed to have. Each test is one claim about
//! durable execution that you should be able to state out loud.

use mini_temporal::prelude::*;
use std::cell::RefCell;
use std::rc::Rc;

type Log = Rc<RefCell<Vec<Execution>>>;

async fn three_steps(ctx: WfContext) -> Result<Payload, String> {
    let a = ctx.activity("a", "1").await.map_err(|e| e.to_string())?;
    let b = ctx.activity("b", a.as_str()).await.map_err(|e| e.to_string())?;
    ctx.timer(100).await;
    let c = ctx.activity("c", b.as_str()).await.map_err(|e| e.to_string())?;
    Ok(c)
}

async fn three_steps_reordered(ctx: WfContext) -> Result<Payload, String> {
    let b = ctx.activity("b", "1").await.map_err(|e| e.to_string())?;
    let a = ctx.activity("a", b.as_str()).await.map_err(|e| e.to_string())?;
    Ok(a)
}

async fn one_failing_step(ctx: WfContext) -> Result<Payload, String> {
    let v = ctx.activity("boom", "x").await.map_err(|e| e.to_string())?;
    Ok(v)
}

fn worker(log: Log, crash: CrashPolicy) -> Worker {
    let mut reg = ActivityRegistry::with_log(log);
    reg.register("a", |i| Ok(format!("a({i})")))
        .register("b", |i| Ok(format!("b({i})")))
        .register("c", |i| Ok(format!("c({i})")))
        .register("boom", |_| Err("card declined".to_string()));
    Worker::new(reg).with_crash(crash)
}

fn count(log: &Log, name: &str) -> usize {
    log.borrow().iter().filter(|e| e.activity_type == name).count()
}

/// The headline claim: replay serves recorded results instead of re-running work.
#[test]
fn recorded_activities_are_never_re_executed() {
    let log: Log = Rc::default();
    let mut history = History::start("in");

    let first = worker(log.clone(), CrashPolicy::AfterRecording(2)).run(three_steps, &mut history);
    assert!(matches!(first, RunOutcome::Crashed { result_recorded: true, .. }));

    let second = worker(log.clone(), CrashPolicy::Never).run(three_steps, &mut history);
    assert_eq!(second, RunOutcome::Completed("c(b(a(1)))".to_string()));

    // Across a crash and a full replay, every side effect happened once.
    assert_eq!(count(&log, "a"), 1);
    assert_eq!(count(&log, "b"), 1);
    assert_eq!(count(&log, "c"), 1);
}

/// Activities are at-least-once. The engine cannot make them exactly-once, and
/// pretending otherwise is the most expensive misunderstanding in this space.
#[test]
fn a_crash_before_recording_re_executes_that_activity() {
    let log: Log = Rc::default();
    let mut history = History::start("in");

    let first = worker(log.clone(), CrashPolicy::BeforeRecording(2)).run(three_steps, &mut history);
    assert!(matches!(first, RunOutcome::Crashed { result_recorded: false, .. }));

    let second = worker(log.clone(), CrashPolicy::Never).run(three_steps, &mut history);
    assert_eq!(second, RunOutcome::Completed("c(b(a(1)))".to_string()));

    assert_eq!(count(&log, "a"), 1, "already recorded before the crash");
    assert_eq!(count(&log, "b"), 2, "side effect escaped, result did not");
    assert_eq!(count(&log, "c"), 1);
}

/// Divergence is caught at the exact command index, before it can do damage.
#[test]
fn changed_workflow_code_is_caught_as_non_determinism() {
    let log: Log = Rc::default();
    let mut history = History::start("in");

    worker(log.clone(), CrashPolicy::AfterRecording(1)).run(three_steps, &mut history);
    let outcome = worker(log.clone(), CrashPolicy::Never).run(three_steps_reordered, &mut history);

    match outcome {
        RunOutcome::NonDeterminism { index, expected, actual } => {
            assert_eq!(index, 0);
            assert!(expected.contains("a("), "{expected}");
            assert!(actual.contains("b("), "{actual}");
        }
        other => panic!("expected non-determinism, got {other:?}"),
    }
    // Nothing from the new code path was executed.
    assert_eq!(count(&log, "b"), 0);
}

/// An already-closed history is immutable: replaying it does nothing at all.
#[test]
fn a_closed_history_is_terminal_and_replay_free() {
    let log: Log = Rc::default();
    let mut history = History::start("in");
    worker(log.clone(), CrashPolicy::Never).run(three_steps, &mut history);

    let events_before = history.len();
    let again = worker(log.clone(), CrashPolicy::Never).run(three_steps, &mut history);

    assert_eq!(again, RunOutcome::Completed("c(b(a(1)))".to_string()));
    assert_eq!(history.len(), events_before, "no new events");
    assert_eq!(count(&log, "a"), 1, "no new side effects");
}

/// Activity failures surface at the await point, as a normal Rust error.
#[test]
fn activity_failure_reaches_the_workflow() {
    let log: Log = Rc::default();
    let mut history = History::start("in");
    let outcome = worker(log, CrashPolicy::Never).run(one_failing_step, &mut history);

    match outcome {
        RunOutcome::Failed(f) => assert!(f.contains("card declined"), "{f}"),
        other => panic!("expected failure, got {other:?}"),
    }
    assert!(history.is_closed());
}

/// Timers are durable facts, not sleeping threads: `TimerStarted` and
/// `TimerFired` are both in history, and no wall-clock time passes.
#[test]
fn timers_are_events() {
    let log: Log = Rc::default();
    let mut history = History::start("in");
    let start = std::time::Instant::now();
    worker(log, CrashPolicy::Never).run(three_steps, &mut history);

    assert!(start.elapsed() < std::time::Duration::from_millis(50));
    assert!(history.events().iter().any(|e| matches!(e.attrs, Event::TimerStarted { .. })));
    assert!(history.events().iter().any(|e| matches!(e.attrs, Event::TimerFired { .. })));
}

/// `commands = f(history)`. Replaying the same history twice yields the exact
/// same command stream -- which is the property everything else rests on.
#[test]
fn replay_is_a_pure_function_of_history() {
    let log: Log = Rc::default();
    let mut history = History::start("in");
    worker(log.clone(), CrashPolicy::AfterRecording(2)).run(three_steps, &mut history);

    let mut h1 = history.clone();
    let mut h2 = history.clone();
    let log1: Log = Rc::default();
    let log2: Log = Rc::default();
    worker(log1.clone(), CrashPolicy::Never).run(three_steps, &mut h1);
    worker(log2.clone(), CrashPolicy::Never).run(three_steps, &mut h2);

    assert_eq!(h1.recorded_commands(), h2.recorded_commands());
    assert_eq!(h1.result(), h2.result());
    assert_eq!(log1.borrow().clone(), log2.borrow().clone());
}
