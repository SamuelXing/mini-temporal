//! An order-fulfilment saga, used to demonstrate the four things that actually
//! matter about durable execution. Run it with no arguments to see all of them:
//!
//! ```text
//! cargo run --example order_saga
//! cargo run --example order_saga -- happy
//! cargo run --example order_saga -- crash-after 2
//! cargo run --example order_saga -- crash-before-record 2
//! cargo run --example order_saga -- nondeterminism
//! ```

use mini_temporal::prelude::*;
use std::cell::RefCell;
use std::rc::Rc;

// ---------------------------------------------------------------------------
// The workflow. Deterministic, no I/O, no clock, no randomness. Every line that
// looks like it does work is really just "emit a command and park here".
// ---------------------------------------------------------------------------

async fn order_saga(ctx: WfContext) -> Result<Payload, String> {
    let order = ctx.input();
    ctx.log(format!("saga started for {order}"));

    let payment = ctx
        .activity("charge_card", order.as_str())
        .await
        .map_err(|e| e.to_string())?;
    ctx.log(format!("card charged: {payment}"));

    let reservation = ctx
        .activity("reserve_inventory", order.as_str())
        .await
        .map_err(|e| e.to_string())?;
    ctx.log(format!("inventory reserved: {reservation}"));

    // A durable timer. The worker may die here and come back an hour later;
    // the timer is an event in history, not a sleeping thread.
    ctx.timer(500).await;
    ctx.log("packing window elapsed");

    let tracking = ctx
        .activity("ship_order", format!("{order}|{payment}|{reservation}"))
        .await
        .map_err(|e| e.to_string())?;

    Ok(format!("shipped {order} tracking={tracking}"))
}

/// Same saga with two steps swapped -- what you get by editing workflow code
/// while executions are in flight.
async fn order_saga_edited(ctx: WfContext) -> Result<Payload, String> {
    let order = ctx.input();

    let reservation = ctx
        .activity("reserve_inventory", order.as_str())
        .await
        .map_err(|e| e.to_string())?;
    let payment = ctx
        .activity("charge_card", order.as_str())
        .await
        .map_err(|e| e.to_string())?;

    ctx.timer(500).await;

    let tracking = ctx
        .activity("ship_order", format!("{order}|{payment}|{reservation}"))
        .await
        .map_err(|e| e.to_string())?;

    Ok(format!("shipped {order} tracking={tracking}"))
}

// ---------------------------------------------------------------------------
// Activities. Arbitrary, effectful, non-deterministic in principle. These ones
// are deterministic only so the demo output is stable.
// ---------------------------------------------------------------------------

fn checksum(s: &str) -> u32 {
    s.bytes().fold(7u32, |acc, b| acc.wrapping_mul(31).wrapping_add(b as u32)) % 100_000
}

fn worker(log: Rc<RefCell<Vec<Execution>>>, crash: CrashPolicy) -> Worker {
    let mut reg = ActivityRegistry::with_log(log);
    reg.register("charge_card", |i| Ok(format!("pay-{}", checksum(&i))))
        .register("reserve_inventory", |i| Ok(format!("res-{}", checksum(&i))))
        .register("ship_order", |i| Ok(format!("trk-{}", checksum(&i))));
    Worker::new(reg).with_crash(crash).with_trace(true)
}

fn rule(title: &str) {
    println!("\n{}", "=".repeat(72));
    println!("== {title}");
    println!("{}", "=".repeat(72));
}

fn report(log: &Rc<RefCell<Vec<Execution>>>) {
    println!("\n   real side effects executed:");
    for name in ["charge_card", "reserve_inventory", "ship_order"] {
        let n = log.borrow().iter().filter(|e| e.activity_type == name).count();
        println!("      {name:<20} {n}x");
    }
}

// ---------------------------------------------------------------------------

fn scenario_happy() {
    rule("HAPPY PATH -- one process, no failures");
    let log = Rc::new(RefCell::new(Vec::new()));
    let mut history = History::start("order-42");

    let outcome = worker(log.clone(), CrashPolicy::Never).run(order_saga, &mut history);

    println!("\n   outcome: {outcome:?}");
    report(&log);
    println!("\n   final history:\n{}", indent(&history.pretty()));
}

fn scenario_crash(crash: CrashPolicy, title: &str, lesson: &str) {
    rule(title);
    // The side-effect log models the outside world: it survives the crash,
    // because the real world does not forget that a card was charged.
    let log = Rc::new(RefCell::new(Vec::new()));
    let mut history = History::start("order-42");

    println!("\n--- process 1 ---");
    let first = worker(log.clone(), crash).run(order_saga, &mut history);
    println!("\n   outcome: {first:?}");
    println!("\n   history that survived the crash:\n{}", indent(&history.pretty()));

    println!("\n--- process 2 (recovery: brand-new worker, same history) ---");
    let second = worker(log.clone(), CrashPolicy::Never).run(order_saga, &mut history);
    println!("\n   outcome: {second:?}");

    report(&log);
    println!("\n   >>> {lesson}");
}

fn scenario_nondeterminism() {
    rule("NON-DETERMINISM -- workflow code edited under a live execution");
    let log = Rc::new(RefCell::new(Vec::new()));
    let mut history = History::start("order-42");

    println!("\n--- process 1: original code, dies after 1 activity ---");
    let first = worker(log.clone(), CrashPolicy::AfterRecording(1)).run(order_saga, &mut history);
    println!("   outcome: {first:?}");

    println!("\n--- process 2: DEPLOYED NEW CODE, resumes the same execution ---");
    let second = worker(log.clone(), CrashPolicy::Never).run(order_saga_edited, &mut history);
    println!("\n   outcome: {second:?}");
    println!(
        "\n   >>> The engine caught it at the exact command index where replay diverged,\n       \
         instead of silently charging the card twice. This is what Temporal's\n       \
         `patched()` / worker versioning exists to avoid."
    );
}

fn indent(s: &str) -> String {
    s.lines().map(|l| format!("      {l}")).collect::<Vec<_>>().join("\n")
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let n = |i: usize| args.get(i).and_then(|s| s.parse::<usize>().ok()).unwrap_or(2);

    match args.first().map(String::as_str) {
        Some("happy") => scenario_happy(),
        Some("crash-after") => scenario_crash(
            CrashPolicy::AfterRecording(n(1)),
            "CRASH AFTER RECORDING -- the result was durable before we died",
            "Every activity ran exactly once. Replay served the recorded results \
             and the workflow resumed mid-function without re-charging anything.",
        ),
        Some("crash-before-record") => scenario_crash(
            CrashPolicy::BeforeRecording(n(1)),
            "CRASH BEFORE RECORDING -- the side effect escaped, the result did not",
            "That activity ran TWICE. Activities are at-least-once, never \
             exactly-once. This is why they must be idempotent -- no engine can \
             close this window, only an idempotency key can.",
        ),
        Some("nondeterminism") => scenario_nondeterminism(),
        _ => {
            scenario_happy();
            scenario_crash(
                CrashPolicy::AfterRecording(2),
                "CRASH AFTER RECORDING -- the result was durable before we died",
                "Every activity ran exactly once. Replay served the recorded results \
                 and the workflow resumed mid-function without re-charging anything.",
            );
            scenario_crash(
                CrashPolicy::BeforeRecording(2),
                "CRASH BEFORE RECORDING -- the side effect escaped, the result did not",
                "reserve_inventory ran TWICE. Activities are at-least-once, never \
                 exactly-once. This is why they must be idempotent -- no engine can \
                 close this window, only an idempotency key can.",
            );
            scenario_nondeterminism();
        }
    }
}
